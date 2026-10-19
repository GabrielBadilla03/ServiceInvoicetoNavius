using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceInvoicetoNavius
{
    public class OracleAtp
    {
        private readonly Action<string> _log;
        private readonly string _csSrc;
        private readonly string _csDst;
        private readonly int _maxDegree;
        private readonly string _ciaFiltro;

        private class FacturaInfo
        {
            public string NumFacturaOracle { get; set; }
            public string Documento { get; set; }
            public string Clave { get; set; }
        }

        private class ResultadoDetalle
        {
            public FacturaInfo Factura { get; set; }
            public bool Ok { get; set; }
            public string Error { get; set; }
        }

        public OracleAtp(
            string walletPath,
            string tnsAliasOrigen,
            string walletPassword,
            string userOrigen, string passOrigen,
            string dataSource11gTns,
            string userDestino, string passDestino,
            string ciaFiltro,
            int maxDegree = 4,
            Action<string> logger = null,
            OpcionesTraslado opciones = null)   // ARREGLO 8
        {
            _log = logger ?? (_ => { });
            _opciones = opciones ?? new OpcionesTraslado();   // ARREGLO 8
            _maxDegree = Math.Max(1, maxDegree);

            if (string.IsNullOrWhiteSpace(ciaFiltro))
                throw new ArgumentException("CiaFiltro viene vacío.");

            _ciaFiltro = ciaFiltro.Trim();

            EnsureOdpConfigured(walletPath);

            _csSrc =
                $"User Id={userOrigen};Password={passOrigen};" +
                $"Data Source={tnsAliasOrigen};" +
                $"Tns_Admin={OracleConfiguration.TnsAdmin};" +
                $"Wallet_Location={OracleConfiguration.WalletLocation};" +
                (string.IsNullOrWhiteSpace(walletPassword) ? "" : $"Wallet_Password={walletPassword};") +
                "Connection Timeout=15;Pooling=true;";

            _csDst =
                $"User Id={userDestino};Password={passDestino};" +
                $"Data Source={dataSource11gTns};" +
                "Connection Timeout=15;Pooling=true;";

            _log($"OracleAtp inicializado. CIA configurada={_ciaFiltro}");
        }

        private void EnsureOdpConfigured(string walletPath)
        {
            if (string.IsNullOrWhiteSpace(walletPath))
                throw new ArgumentException("WalletPath viene vacío.");

            if (!Directory.Exists(walletPath))
                throw new DirectoryNotFoundException($"No existe WalletPath: {walletPath}");

            OracleConfiguration.TnsAdmin = walletPath;
            OracleConfiguration.WalletLocation = walletPath;
        }

        private OracleConnection OpenSrc()
        {
            var c = new OracleConnection(_csSrc);
            c.Open();
            return c;
        }

        private OracleConnection OpenDst()
        {
            var c = new OracleConnection(_csDst);
            c.Open();
            return c;
        }


        private sealed class ColumnaDestinoInfo
        {
            public string ColumnName { get; set; }
            public string DataType { get; set; }
            public int? DataLength { get; set; }
            public int? DataPrecision { get; set; }
            public int? DataScale { get; set; }
            public int? CharLength { get; set; }
            public string CharUsed { get; set; }
            public string Nullable { get; set; }
        }

        /// <summary>
        /// Ejecuta OracleBulkCopy con validación previa de tabla, columnas, tipos y valores.
        /// Cuando existe una incompatibilidad, el log indica:
        /// - tabla destino
        /// - columna
        /// - fila
        /// - factura/documento/línea
        /// - tipo origen
        /// - tipo destino
        /// - valor que produjo el problema
        /// </summary>
        private void BulkCopyConDiagnostico(
            DataTable datos,
            string tablaDestino,
            string contexto)
        {
            if (datos == null)
                throw new ArgumentNullException(nameof(datos));

            if (datos.Rows.Count == 0)
            {
                _log($"Bulk omitido porque no hay filas | Tabla={tablaDestino} | Contexto={contexto}");
                return;
            }

            _log("-------------------------------------------");
            _log($"Preparando OracleBulkCopy | Tabla={tablaDestino} | Contexto={contexto} | Filas={datos.Rows.Count} | Columnas={datos.Columns.Count}");

            using (var conDst = OpenDst())
            {
                Dictionary<string, ColumnaDestinoInfo> columnasDestino =
                    ObtenerColumnasDestino(conDst, tablaDestino);

                DataTable datosPreparados = PrepararDatosParaDestino(
                    conDst,
                    datos,
                    tablaDestino,
                    contexto,
                    columnasDestino);

                try
                {
                    using (var bulk = new OracleBulkCopy(
                        conDst,
                        OracleBulkCopyOptions.UseInternalTransaction))
                    {
                        bulk.DestinationTableName = tablaDestino;
                        bulk.BulkCopyTimeout = 0;
                        bulk.BatchSize = Math.Max(1, datosPreparados.Rows.Count);

                        foreach (DataColumn col in datosPreparados.Columns)
                            bulk.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                        bulk.WriteToServer(datosPreparados);
                    }

                    _log($"OracleBulkCopy OK | Tabla={tablaDestino} | Contexto={contexto} | Filas={datosPreparados.Rows.Count}");
                    _log("-------------------------------------------");
                }
                catch (Exception ex)
                {
                    _log("===========================================");
                    _log("ERROR ORACLE BULK COPY");
                    _log($"Tabla destino : {tablaDestino}");
                    _log($"Contexto      : {contexto}");
                    _log($"Filas         : {datosPreparados.Rows.Count}");
                    _log($"Mensaje       : {ex.Message}");

                    if (ex.InnerException != null)
                        _log($"InnerException: {ex.InnerException.Message}");

                    _log($"StackTrace    : {ex.StackTrace}");
                    _log("===========================================");

                    throw new Exception(
                        $"Error OracleBulkCopy | Tabla={tablaDestino} | Contexto={contexto} | {ex.Message}",
                        ex);
                }
            }
        }

        private Dictionary<string, ColumnaDestinoInfo> ObtenerColumnasDestino(
            OracleConnection conDst,
            string tablaDestino)
        {
            string owner;
            string tableName;
            SepararOwnerYTabla(tablaDestino, out owner, out tableName);

            var columnas = new Dictionary<string, ColumnaDestinoInfo>(
                StringComparer.OrdinalIgnoreCase);

            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;

                if (string.IsNullOrWhiteSpace(owner))
                {
                    cmd.CommandText = @"
                        SELECT COLUMN_NAME,
                               DATA_TYPE,
                               DATA_LENGTH,
                               DATA_PRECISION,
                               DATA_SCALE,
                               CHAR_LENGTH,
                               CHAR_USED,
                               NULLABLE
                          FROM USER_TAB_COLUMNS
                         WHERE TABLE_NAME = :pTABLE_NAME
                         ORDER BY COLUMN_ID";

                    cmd.Parameters.Add(
                        ":pTABLE_NAME",
                        OracleDbType.Varchar2,
                        tableName,
                        ParameterDirection.Input);
                }
                else
                {
                    cmd.CommandText = @"
                        SELECT COLUMN_NAME,
                               DATA_TYPE,
                               DATA_LENGTH,
                               DATA_PRECISION,
                               DATA_SCALE,
                               CHAR_LENGTH,
                               CHAR_USED,
                               NULLABLE
                          FROM ALL_TAB_COLUMNS
                         WHERE OWNER = :pOWNER
                           AND TABLE_NAME = :pTABLE_NAME
                         ORDER BY COLUMN_ID";

                    cmd.Parameters.Add(
                        ":pOWNER",
                        OracleDbType.Varchar2,
                        owner,
                        ParameterDirection.Input);

                    cmd.Parameters.Add(
                        ":pTABLE_NAME",
                        OracleDbType.Varchar2,
                        tableName,
                        ParameterDirection.Input);
                }

                using (var rd = cmd.ExecuteReader())
                {
                    while (rd.Read())
                    {
                        var info = new ColumnaDestinoInfo
                        {
                            ColumnName = Convert.ToString(rd["COLUMN_NAME"]),
                            DataType = Convert.ToString(rd["DATA_TYPE"]),
                            DataLength = ToNullableInt(rd["DATA_LENGTH"]),
                            DataPrecision = ToNullableInt(rd["DATA_PRECISION"]),
                            DataScale = ToNullableInt(rd["DATA_SCALE"]),
                            CharLength = ToNullableInt(rd["CHAR_LENGTH"]),
                            CharUsed = Convert.ToString(rd["CHAR_USED"]),
                            Nullable = Convert.ToString(rd["NULLABLE"])
                        };

                        columnas[info.ColumnName] = info;
                    }
                }
            }

            if (columnas.Count == 0)
            {
                throw new Exception(
                    $"No se pudo leer la estructura de la tabla destino {tablaDestino}. " +
                    "Verifique que la tabla exista y que el usuario tenga permisos.");
            }

            return columnas;
        }

        private DataTable PrepararDatosParaDestino(
            OracleConnection conDst,
            DataTable origen,
            string tablaDestino,
            string contexto,
            Dictionary<string, ColumnaDestinoInfo> columnasDestino)
        {
            var preparado = new DataTable();

            foreach (DataColumn columnaOrigen in origen.Columns)
            {
                ColumnaDestinoInfo columnaDestino;

                if (!columnasDestino.TryGetValue(
                    columnaOrigen.ColumnName,
                    out columnaDestino))
                {
                    string mensaje =
                        $"La columna no existe en destino | " +
                        $"Tabla={tablaDestino} | " +
                        $"Columna={columnaOrigen.ColumnName} | " +
                        $"Contexto={contexto}";

                    _log(mensaje);
                    throw new Exception(mensaje);
                }

                Type tipoDestinoNet = ObtenerTipoNetDestino(columnaDestino);
                preparado.Columns.Add(columnaOrigen.ColumnName, tipoDestinoNet);

                if (!TiposCompatibles(columnaOrigen.DataType, tipoDestinoNet))
                {
                    _log(
                        $"DIFERENCIA DE TIPO | " +
                        $"Tabla={tablaDestino} | " +
                        $"Columna={columnaOrigen.ColumnName} | " +
                        $"OrigenNET={columnaOrigen.DataType.FullName} | " +
                        $"DestinoOracle={DescribirTipoOracle(columnaDestino)} | " +
                        $"DestinoNET={tipoDestinoNet.FullName}");
                }
            }

            for (int filaIndex = 0; filaIndex < origen.Rows.Count; filaIndex++)
            {
                DataRow filaOrigen = origen.Rows[filaIndex];
                DataRow filaDestino = preparado.NewRow();

                foreach (DataColumn columnaOrigen in origen.Columns)
                {
                    ColumnaDestinoInfo columnaDestino =
                        columnasDestino[columnaOrigen.ColumnName];

                    object valorOrigen = filaOrigen[columnaOrigen];

                    try
                    {
                        if ((valorOrigen == null || valorOrigen == DBNull.Value) &&
                            string.Equals(
                                columnaDestino.Nullable,
                                "N",
                                StringComparison.OrdinalIgnoreCase))
                        {
                            throw new InvalidOperationException(
                                "La columna destino es NOT NULL y el valor origen viene NULL.");
                        }

                        filaDestino[columnaOrigen.ColumnName] =
                            ConvertirValorParaDestino(
                                conDst,
                                valorOrigen,
                                columnaDestino);
                    }
                    catch (Exception ex)
                    {
                        string identificador = ObtenerIdentificadorFila(filaOrigen);
                        string valor = FormatearValorParaLog(valorOrigen);

                        _log("===========================================");
                        _log("ERROR DE COLUMNA DETECTADO ANTES DEL BULK COPY");
                        _log($"Tabla destino : {tablaDestino}");
                        _log($"Contexto      : {contexto}");
                        _log($"Fila DataTable: {filaIndex + 1}");
                        _log($"Identificador : {identificador}");
                        _log($"Columna       : {columnaOrigen.ColumnName}");
                        _log($"Tipo origen   : {columnaOrigen.DataType.FullName}");
                        _log($"Tipo destino  : {DescribirTipoOracle(columnaDestino)}");
                        _log($"Valor         : [{valor}]");
                        _log($"Detalle       : {ex.Message}");
                        _log("===========================================");

                        throw new Exception(
                            $"Error de datos | Tabla={tablaDestino} | " +
                            $"Columna={columnaOrigen.ColumnName} | " +
                            $"Fila={filaIndex + 1} | " +
                            $"{identificador} | " +
                            $"Valor=[{valor}] | " +
                            $"Destino={DescribirTipoOracle(columnaDestino)} | " +
                            $"Detalle={ex.Message}",
                            ex);
                    }
                }

                preparado.Rows.Add(filaDestino);
            }

            return preparado;
        }

        private object ConvertirValorParaDestino(
            OracleConnection conDst,
            object valor,
            ColumnaDestinoInfo destino)
        {
            if (valor == null || valor == DBNull.Value)
                return DBNull.Value;

            string dataType = (destino.DataType ?? "").ToUpperInvariant();

            if (EsTipoTexto(dataType))
            {
                string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);
                ValidarLongitudTexto(conDst, texto, destino);
                return texto;
            }

            if (dataType == "NUMBER" ||
                dataType == "DECIMAL" ||
                dataType == "NUMERIC")
            {
                decimal numero = ConvertirADecimal(valor);
                ValidarPrecisionNumero(numero, destino);
                return numero;
            }

            if (dataType == "FLOAT" ||
                dataType == "BINARY_FLOAT" ||
                dataType == "BINARY_DOUBLE")
            {
                return ConvertirADouble(valor);
            }

            if (dataType == "DATE" || dataType.StartsWith("TIMESTAMP"))
            {
                return ConvertirAFecha(valor);
            }

            if (dataType == "RAW" || dataType == "LONG RAW" || dataType == "BLOB")
            {
                if (valor is byte[])
                    return valor;

                throw new FormatException(
                    $"Se esperaba un arreglo de bytes para {dataType}, " +
                    $"pero se recibió {valor.GetType().FullName}.");
            }

            return valor;
        }

        private decimal ConvertirADecimal(object valor)
        {
            if (valor is decimal)
                return (decimal)valor;

            if (valor is byte ||
                valor is short ||
                valor is int ||
                valor is long ||
                valor is sbyte ||
                valor is ushort ||
                valor is uint ||
                valor is ulong ||
                valor is float ||
                valor is double)
            {
                return Convert.ToDecimal(valor, CultureInfo.InvariantCulture);
            }

            string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);

            if (string.IsNullOrWhiteSpace(texto))
                throw new FormatException("El valor viene vacío y la columna destino es NUMBER.");

            texto = texto.Trim();

            decimal numero;

            if (decimal.TryParse(
                texto,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out numero))
            {
                return numero;
            }

            if (decimal.TryParse(
                texto,
                NumberStyles.Any,
                CultureInfo.CurrentCulture,
                out numero))
            {
                return numero;
            }

            throw new FormatException(
                $"El valor '{texto}' no se puede convertir a NUMBER.");
        }

        private void ValidarPrecisionNumero(
            decimal numero,
            ColumnaDestinoInfo destino)
        {
            if (!destino.DataPrecision.HasValue)
                return;

            int precision = destino.DataPrecision.Value;
            int escala = destino.DataScale ?? 0;

            if (precision <= 0 || escala < 0)
                return;

            string texto = Math.Abs(numero).ToString(
                "0.#############################",
                CultureInfo.InvariantCulture);

            string[] partes = texto.Split('.');
            string enteros = partes[0].TrimStart('0');

            int digitosEnteros = string.IsNullOrEmpty(enteros)
                ? 0
                : enteros.Length;

            int maximoEnteros = precision - escala;

            if (digitosEnteros > maximoEnteros)
            {
                throw new OverflowException(
                    $"El número excede la precisión del destino. " +
                    $"DígitosEnteros={digitosEnteros}, " +
                    $"MáximoEnteros={maximoEnteros}, " +
                    $"Tipo={DescribirTipoOracle(destino)}.");
            }
        }

        private double ConvertirADouble(object valor)
        {
            if (valor is double)
                return (double)valor;

            string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);

            if (string.IsNullOrWhiteSpace(texto))
                throw new FormatException("El valor viene vacío y la columna destino es numérica.");

            double numero;

            if (double.TryParse(
                texto,
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out numero))
            {
                return numero;
            }

            if (double.TryParse(
                texto,
                NumberStyles.Any,
                CultureInfo.CurrentCulture,
                out numero))
            {
                return numero;
            }

            throw new FormatException(
                $"El valor '{texto}' no se puede convertir a {valor.GetType().Name}.");
        }

        private DateTime ConvertirAFecha(object valor)
        {
            if (valor is DateTime)
                return (DateTime)valor;

            string texto = Convert.ToString(valor, CultureInfo.InvariantCulture);

            if (string.IsNullOrWhiteSpace(texto))
                throw new FormatException("El valor viene vacío y la columna destino es DATE/TIMESTAMP.");

            DateTime fecha;

            if (DateTime.TryParse(
                texto,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces,
                out fecha))
            {
                return fecha;
            }

            if (DateTime.TryParse(
                texto,
                CultureInfo.CurrentCulture,
                DateTimeStyles.AllowWhiteSpaces,
                out fecha))
            {
                return fecha;
            }

            throw new FormatException(
                $"El valor '{texto}' no se puede convertir a DATE/TIMESTAMP.");
        }

        private void ValidarLongitudTexto(
            OracleConnection conDst,
            string texto,
            ColumnaDestinoInfo destino)
        {
            if (texto == null)
                return;

            string dataType = (destino.DataType ?? "").ToUpperInvariant();

            if (dataType == "CLOB" ||
                dataType == "NCLOB" ||
                dataType == "LONG")
            {
                return;
            }

            if (string.Equals(destino.CharUsed, "C", StringComparison.OrdinalIgnoreCase))
            {
                int limiteCaracteres = destino.CharLength ?? destino.DataLength ?? int.MaxValue;

                if (texto.Length > limiteCaracteres)
                {
                    throw new InvalidOperationException(
                        $"Longitud excedida. Caracteres={texto.Length}, Máximo={limiteCaracteres}.");
                }

                return;
            }

            int limiteBytes = destino.DataLength ?? int.MaxValue;

            if (texto.Length > limiteBytes)
            {
                throw new InvalidOperationException(
                    $"Longitud excedida. Caracteres={texto.Length}, MáximoBytes={limiteBytes}.");
            }

            int bytesUtf8 = Encoding.UTF8.GetByteCount(texto);

            if (bytesUtf8 > limiteBytes)
            {
                int bytesOracle = ObtenerLongitudBytesOracle(conDst, texto);

                if (bytesOracle > limiteBytes)
                {
                    throw new InvalidOperationException(
                        $"Longitud excedida. BytesOracle={bytesOracle}, MáximoBytes={limiteBytes}.");
                }
            }
        }

        private int ObtenerLongitudBytesOracle(
            OracleConnection conDst,
            string texto)
        {
            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = "SELECT LENGTHB(:pVALOR) FROM DUAL";
                cmd.Parameters.Add(
                    ":pVALOR",
                    OracleDbType.Varchar2,
                    texto,
                    ParameterDirection.Input);

                object resultado = cmd.ExecuteScalar();
                return resultado == null || resultado == DBNull.Value
                    ? 0
                    : Convert.ToInt32(resultado);
            }
        }

        private Type ObtenerTipoNetDestino(ColumnaDestinoInfo destino)
        {
            string dataType = (destino.DataType ?? "").ToUpperInvariant();

            if (EsTipoTexto(dataType))
                return typeof(string);

            if (dataType == "NUMBER" ||
                dataType == "DECIMAL" ||
                dataType == "NUMERIC")
            {
                return typeof(decimal);
            }

            if (dataType == "FLOAT" ||
                dataType == "BINARY_FLOAT" ||
                dataType == "BINARY_DOUBLE")
            {
                return typeof(double);
            }

            if (dataType == "DATE" || dataType.StartsWith("TIMESTAMP"))
                return typeof(DateTime);

            if (dataType == "RAW" || dataType == "LONG RAW" || dataType == "BLOB")
                return typeof(byte[]);

            return typeof(object);
        }

        private bool EsTipoTexto(string dataType)
        {
            return dataType == "VARCHAR2" ||
                   dataType == "VARCHAR" ||
                   dataType == "CHAR" ||
                   dataType == "NVARCHAR2" ||
                   dataType == "NCHAR" ||
                   dataType == "CLOB" ||
                   dataType == "NCLOB" ||
                   dataType == "LONG" ||
                   dataType == "ROWID" ||
                   dataType == "UROWID";
        }

        private bool TiposCompatibles(Type origen, Type destino)
        {
            if (origen == destino)
                return true;

            if (destino == typeof(decimal) &&
                (origen == typeof(byte) ||
                 origen == typeof(short) ||
                 origen == typeof(int) ||
                 origen == typeof(long) ||
                 origen == typeof(float) ||
                 origen == typeof(double) ||
                 origen == typeof(decimal)))
            {
                return true;
            }

            if (destino == typeof(double) &&
                (origen == typeof(float) ||
                 origen == typeof(double) ||
                 origen == typeof(decimal)))
            {
                return true;
            }

            return false;
        }

        private string DescribirTipoOracle(ColumnaDestinoInfo info)
        {
            string tipo = info.DataType ?? "DESCONOCIDO";

            if (string.Equals(tipo, "NUMBER", StringComparison.OrdinalIgnoreCase))
            {
                if (info.DataPrecision.HasValue && info.DataScale.HasValue)
                    return $"NUMBER({info.DataPrecision.Value},{info.DataScale.Value})";

                if (info.DataPrecision.HasValue)
                    return $"NUMBER({info.DataPrecision.Value})";

                if (info.DataScale.HasValue)
                    return $"NUMBER(*,{info.DataScale.Value})";

                return "NUMBER";
            }

            if (EsTipoTexto(tipo.ToUpperInvariant()) &&
                !string.Equals(tipo, "CLOB", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(tipo, "NCLOB", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(tipo, "LONG", StringComparison.OrdinalIgnoreCase))
            {
                int longitud = info.CharLength ?? info.DataLength ?? 0;
                string unidad = string.Equals(info.CharUsed, "C", StringComparison.OrdinalIgnoreCase)
                    ? "CHAR"
                    : "BYTE";

                return $"{tipo}({longitud} {unidad})";
            }

            return tipo;
        }

        private string ObtenerIdentificadorFila(DataRow fila)
        {
            var partes = new List<string>();

            AgregarIdentificadorSiExiste(fila, partes, "NUM_FACTURA_ORACLE");
            AgregarIdentificadorSiExiste(fila, partes, "DOCUMENTO");
            AgregarIdentificadorSiExiste(fila, partes, "CLAVE");
            AgregarIdentificadorSiExiste(fila, partes, "LINEADETALLE");
            AgregarIdentificadorSiExiste(fila, partes, "CODIGO");
            AgregarIdentificadorSiExiste(fila, partes, "CODIGO_COD");

            return partes.Count == 0
                ? "Sin columnas identificadoras"
                : string.Join(" | ", partes);
        }

        private void AgregarIdentificadorSiExiste(
            DataRow fila,
            List<string> partes,
            string columna)
        {
            if (!fila.Table.Columns.Contains(columna))
                return;

            object valor = fila[columna];

            if (valor == null || valor == DBNull.Value)
                return;

            string texto = Convert.ToString(valor);

            if (!string.IsNullOrWhiteSpace(texto))
                partes.Add($"{columna}={texto.Trim()}");
        }

        private string FormatearValorParaLog(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return "<NULL>";

            string texto;

            if (valor is byte[])
                texto = $"<BYTE[{((byte[])valor).Length}]>";
            else
                texto = Convert.ToString(valor, CultureInfo.InvariantCulture);

            if (texto == null)
                return "<NULL>";

            texto = texto.Replace("\r", " ").Replace("\n", " ");

            const int maximo = 500;

            return texto.Length <= maximo
                ? texto
                : texto.Substring(0, maximo) + "...";
        }

        private int? ToNullableInt(object valor)
        {
            if (valor == null || valor == DBNull.Value)
                return null;

            return Convert.ToInt32(valor);
        }

        private void SepararOwnerYTabla(
            string tablaDestino,
            out string owner,
            out string tableName)
        {
            if (string.IsNullOrWhiteSpace(tablaDestino))
                throw new ArgumentException("El nombre de la tabla destino viene vacío.");

            string limpio = tablaDestino.Trim().Replace("\"", "");
            string[] partes = limpio.Split('.');

            if (partes.Length == 1)
            {
                owner = null;
                tableName = partes[0].ToUpperInvariant();
                return;
            }

            if (partes.Length == 2)
            {
                owner = partes[0].ToUpperInvariant();
                tableName = partes[1].ToUpperInvariant();
                return;
            }

            throw new ArgumentException(
                $"Nombre de tabla destino inválido: {tablaDestino}");
        }

        private void MarcarFacturaConError(FacturaInfo factura, string error)
        {
            try
            {
                using (var conSrc = OpenSrc())
                using (var cmd = conSrc.CreateCommand())
                {
                    cmd.BindByName = true;
                    cmd.CommandText = @"
                        UPDATE VENDOCENCFED
                           SET INDTRASLADO = 'E',
                               MENSAJE_HACIENDA = SUBSTR(:pERROR, 1, 4000)
                         WHERE NUM_FACTURA_ORACLE = :pNUM_FACTURA_ORACLE
                           AND CIA = :pCIA
                           AND NVL(INDTRASLADO,'N') = 'N'";  // ARREGLO 4: solo filas pendientes

                    cmd.Parameters.Add(":pERROR", OracleDbType.Varchar2, error, ParameterDirection.Input);
                    cmd.Parameters.Add(":pNUM_FACTURA_ORACLE", OracleDbType.Varchar2, factura.NumFacturaOracle, ParameterDirection.Input);
                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    int afectados = cmd.ExecuteNonQuery();

                    _log($"ATP marcado INDTRASLADO='E' | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle} | Afectados={afectados}");
                }
            }
            catch (Exception ex)
            {
                _log($"ERROR marcando INDTRASLADO='E' | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle} | Error={ex.Message}");
            }
        }

        private void ValidarIndicesDestino()
        {
            using (var con = OpenDst())
            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = @"
                    SELECT INDEX_NAME, TABLE_NAME, STATUS
                    FROM USER_INDEXES
                    WHERE TABLE_NAME IN 
                    (
                        'XXORA_VENDOCENCFED',
                        'XXORA_VENDOCDETFED',
                        'XXORA_VENDOCDETIMPFED'
                    )
                    AND STATUS <> 'VALID'";

                using (var rd = cmd.ExecuteReader())
                {
                    var errores = new List<string>();

                    while (rd.Read())
                    {
                        errores.Add(
                            $"Indice={rd["INDEX_NAME"]}, Tabla={rd["TABLE_NAME"]}, Estado={rd["STATUS"]}"
                        );
                    }

                    if (errores.Count > 0)
                    {
                        string msg = "Existen índices inválidos o inutilizables en destino 11g: " +
                                     string.Join(" | ", errores);

                        _log(msg);
                        throw new Exception(msg);
                    }
                }
            }
        }

        private bool ExisteCabeceraEnDestino(FacturaInfo factura, out string error)
        {
            error = null;

            using (var conDst = OpenDst())
            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = @"
                    SELECT COUNT(1)
                    FROM XXORA_VENDOCENCFED
                    WHERE NUM_FACTURA_ORACLE = :pNUM_FACTURA_ORACLE
                      AND CIA = :pCIA";

                cmd.Parameters.Add(":pNUM_FACTURA_ORACLE", OracleDbType.Varchar2, factura.NumFacturaOracle, ParameterDirection.Input);
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                int existe = Convert.ToInt32(cmd.ExecuteScalar());

                if (existe > 0)
                {
                    error = $"Documento ya existe en cabecera destino. NUM_FACTURA_ORACLE={factura.NumFacturaOracle}";
                    return true;
                }
            }

            return false;
        }

        private bool ExisteDetalleEnDestino(DataTable dtDet, FacturaInfo factura, out string error)
        {
            error = null;

            if (dtDet == null || dtDet.Rows.Count == 0)
                return false;

            using (var conDst = OpenDst())
            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = @"
                    SELECT COUNT(1)
                    FROM XXORA_VENDOCDETFED
                    WHERE CIA = :pCIA
                      AND NUM_FACTURA_ORACLE = :pNUM_FACTURA_ORACLE
                      AND LINEADETALLE = :pLINEADETALLE";

                var pCia = cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2);
                var pNum = cmd.Parameters.Add(":pNUM_FACTURA_ORACLE", OracleDbType.Varchar2);
                var pLinea = cmd.Parameters.Add(":pLINEADETALLE", OracleDbType.Varchar2);

                foreach (DataRow row in dtDet.Rows)
                {
                    string cia = row["CIA"]?.ToString()?.Trim();
                    string numFacturaOracle = row["NUM_FACTURA_ORACLE"]?.ToString()?.Trim();
                    string lineaDetalle = row["LINEADETALLE"]?.ToString()?.Trim();

                    pCia.Value = cia;
                    pNum.Value = numFacturaOracle;
                    pLinea.Value = lineaDetalle;

                    int existe = Convert.ToInt32(cmd.ExecuteScalar());

                    if (existe > 0)
                    {
                        error =
                            $"Error PK duplicada detalle. Ya existe en destino. " +
                            $"CIA={cia}, NUM_FACTURA_ORACLE={numFacturaOracle}, LINEADETALLE={lineaDetalle}";

                        return true;
                    }
                }
            }

            return false;
        }

        private bool ExisteImpuestoEnDestino(DataTable dtImp, FacturaInfo factura, out string error)
        {
            error = null;

            if (dtImp == null || dtImp.Rows.Count == 0)
                return false;

            using (var conDst = OpenDst())
            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = @"
                    SELECT COUNT(1)
                    FROM XXORA_VENDOCDETIMPFED
                    WHERE CIA = :pCIA
                      AND NUM_FACTURA_ORACLE = :pNUM_FACTURA_ORACLE
                      AND LINEADETALLE = :pLINEADETALLE
                      AND CODIGO = :pCODIGO
                      AND CODIGOTARIFA = :pCODIGOTARIFA";

                var pCia = cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2);
                var pNum = cmd.Parameters.Add(":pNUM_FACTURA_ORACLE", OracleDbType.Varchar2);
                var pLinea = cmd.Parameters.Add(":pLINEADETALLE", OracleDbType.Varchar2);
                var pCodigo = cmd.Parameters.Add(":pCODIGO", OracleDbType.Varchar2);
                var pCodigoTarifa = cmd.Parameters.Add(":pCODIGOTARIFA", OracleDbType.Varchar2);

                foreach (DataRow row in dtImp.Rows)
                {
                    string cia = row["CIA"]?.ToString()?.Trim();
                    string numFacturaOracle = row["NUM_FACTURA_ORACLE"]?.ToString()?.Trim();
                    string lineaDetalle = row["LINEADETALLE"]?.ToString()?.Trim();
                    string codigo = row["CODIGO"]?.ToString()?.Trim();
                    string codigoTarifa = row["CODIGOTARIFA"]?.ToString()?.Trim();

                    pCia.Value = cia;
                    pNum.Value = numFacturaOracle;
                    pLinea.Value = lineaDetalle;
                    pCodigo.Value = codigo;
                    pCodigoTarifa.Value = codigoTarifa;

                    int existe = Convert.ToInt32(cmd.ExecuteScalar());

                    if (existe > 0)
                    {
                        error =
                            $"Error PK duplicada impuesto. Ya existe en destino. " +
                            $"CIA={cia}, NUM_FACTURA_ORACLE={numFacturaOracle}, LINEADETALLE={lineaDetalle}, CODIGO={codigo}, CODIGOTARIFA={codigoTarifa}";

                        return true;
                    }
                }
            }

            return false;
        }

        // =====================================================================================
        // TRASLADO ATP -> 11g  (ARREGLOS 1 a 4)
        // Cada factura se traslada completa o no se traslada:
        //   detalle -> impuestos -> encabezado (de último) -> COMMIT, en UNA transacción.
        // OracleBulkCopy no puede participar en una transacción externa, por eso aquí se
        // usa INSERT con array binding (InsertarArrayBind).
        // =====================================================================================

        // Valores configurables desde App.config (ver Service1.LeerOpciones).
        // Si la llave no existe en el .config, se usa el valor por defecto de aquí.
        public sealed class OpcionesTraslado
        {
            // ARREGLO 8: máximo de facturas que se trasladan por ciclo (Service1 lo limita a 1..100).
            public int MaxFacturasPorCiclo { get; set; } = 10;

            // ARREGLO 2: validar que el total del encabezado cuadre con la suma de líneas.
            // Se puede apagar desde el .config sin recompilar si diera falsos positivos.
            public bool ValidarMontos { get; set; } = true;

            // Diferencia máxima aceptada entre el total del encabezado y la suma de las líneas.
            public decimal ToleranciaMonto { get; set; } = 1.00m;

            // Ciclos que se espera a que la integración termine de cargar una factura en ATP
            // antes de marcarla con error (con IntervalMs=240000, 5 ciclos = 20 minutos).
            public int MaxCiclosEspera { get; set; } = 5;

            // REVISION v5: ciclos que se deja descansar una factura que falló por un motivo
            // que no se corrige solo (repetida, copia distinta en 11g, datos inconsistentes).
            // Con IntervalMs=240000, 15 ciclos = 1 hora.
            public int CiclosReintentoRevision { get; set; } = 15;

            // REVISION v5: tiempo máximo de GuardarFacturas en segundos (0 = sin límite).
            // Ojo: si se corta, el paquete revierte TODO el lote (consecutivos ya gastados).
            public int TimeoutGuardarFacturasSeg { get; set; } = 600;
        }

        private readonly OpcionesTraslado _opciones;

        // ARREGLO 8: lo activa Service1.OnStop para no empezar facturas nuevas al detenerse.
        private volatile bool _detener;

        public void SolicitarDetencion()
        {
            _detener = true;
        }

        private readonly ConcurrentDictionary<string, int> _ciclosEspera =
            new ConcurrentDictionary<string, int>();

        // REVISION v5: facturas que no se pudieron trasladar y siguen en INDTRASLADO='N'.
        // Valor = número de ciclo a partir del cual se puede volver a intentar.
        // Sin esto, como ATP las devuelve siempre primero (ORDER BY ruta), unas pocas
        // facturas trabadas ocupaban TODO el lote y el servicio dejaba de trasladar.
        private readonly ConcurrentDictionary<string, int> _enRevision =
            new ConcurrentDictionary<string, int>();

        private int _cicloActual = 0;

        private readonly ConcurrentDictionary<string, Dictionary<string, ColumnaDestinoInfo>> _cacheColumnas =
            new ConcurrentDictionary<string, Dictionary<string, ColumnaDestinoInfo>>(StringComparer.OrdinalIgnoreCase);

        private enum ResultadoTraslado { Ok, Pendiente, Error }

        private sealed class GrupoFactura
        {
            public string NumFacturaOracle { get; set; }
            public List<DataRow> Filas { get; set; }
            public DataTable Cabecera { get; set; }
        }

        // Mismo filtro que usa GuardarFacturas (codigo_tipo = tipodoc): si el paquete no va
        // a tomar una línea, el servicio tampoco la cuenta como detalle.
        private const string SqlDetalleAtp = @"
            SELECT CIA, CLAVE, LINEADETALLE,
                   CODIGO_TIPO, CODIGO_COD, CANTIDAD,
                   UNIDADMEDIDA, UNIDADMEDIDACOMERCIAL, DETALLE,
                   PRECIOUNITARIO, MONTOTOTAL, MONTODESCUENTO,
                   NATURALEZADESCUENTO, SUBTOTAL, TOTALIMPUESTOS,
                   MONTOTOTALLINEA, BASEIMPONIBLE, PARTIDAARANCELARIA,
                   COD_CABYS, SUGERIDO, MED_IMP,
                   PRECIO_IMP, CLASIFICACION, CODIGO_DESCUENTO,
                   IVACOBRADOFABRICA, IVAASUMIDOEMISOR, REG_MEDICAMENTO,
                   FOR_FARMACEUTICA, CODIGO_COMBO, CODIGO_BONIF, LINEACOMBO,
                   ID_FACTURA_ORACLE, NUM_FACTURA_ORACLE, BULTOS, COMENTARIOS, LOTE,
                   source_schedule_number, CODIGO_IMPUESTO, TASA_IMPUESTO
              FROM VENDOCDETFED
             WHERE NUM_FACTURA_ORACLE = :p
               AND CIA = :pCIA
               AND CODIGO_TIPO = :pTIPO";

        private const string SqlImpuestoAtp = @"
            SELECT CIA, CLAVE, LINEADETALLE,
                   CODIGO, TARIFA, MONTO,
                   EXONERA_TIPODOC, EXONERA_NUMDOC, EXONERA_NOMINSTIT,
                   EXONERA_FECHAEMISION, EXONERA_MONTOIMP, EXONERA_PORCCOMPRA,
                   CODIGOTARIFA, FACTORIVA, CODIGO_COMBO,
                   CODIGO_BONIF, EXONERA_ARTICULO, EXONERA_INCISO, NUM_FACTURA_ORACLE
              FROM VENDOCDETIMPFED
             WHERE NUM_FACTURA_ORACLE = :p
               AND CIA = :pCIA";

        public int VENDOCENCFED()
        {
            ValidarIndicesDestino();

            // SELECT de encabezados: SIN CAMBIOS respecto a la versión original.
            const string selectBase = @"
                SELECT CIA, CLAVE, TIPODOC,
                       NUMEROCONSECUTIVO, FECHAEMISION, EMISOR_NOMBRE,
                       EMISOR_ID_TIPO, EMISOR_ID_NUMERO, EMISOR_NOMBRECOMERCIAL,
                       EMISOR_UBIC_PROVINCIA, EMISOR_UBIC_CANTON, EMISOR_UBIC_DISTRITO,
                       EMISOR_UBIC_BARRIO, EMISOR_UBIC_OTRASSENAS, EMISOR_TEL_CODIGOPAIS,
                       REGEXP_REPLACE(EMISOR_TEL_NUMTELEFONO, '[^0-9]', '') AS EMISOR_TEL_NUMTELEFONO,
                       EMISOR_FAX_CODIGOPAIS, EMISOR_FAX_NUMTELEFONO,
                       EMISOR_CORREOELECTRONICO, RECEPTOR_NOMBRE, RECEPTOR_ID_TIPO,
                       RECEPTOR_ID_NUMERO, RECEPTOR_IDEXTRANJERO, RECEPTOR_NOMBRECOMERCIAL,
                       RECEPTOR_UBIC_PROVINCIA, RECEPTOR_UBIC_CANTON, RECEPTOR_UBIC_DISTRITO,
                       RECEPTOR_UBIC_BARRIO, RECEPTOR_UBIC_OTRASSENAS, RECEPTOR_TEL_CODIGOPAIS,
                       RECEPTOR_TEL_NUMTELEFONO, RECEPTOR_FAX_CODIGOPAIS, RECEPTOR_FAX_NUMTELEFONO,
                       RECEPTOR_CORREOELECTRONICO, CONDICIONVENTA, PLAZOCREDITO,
                       CODIGOMONEDA, TIPOCAMBIO, TOTALSERVGRAVADOS,
                       TOTALSERVEXENTOS, TOTALMERCANCIASGRAVADAS, TOTALMERCANCIASEXENTAS,
                       TOTALGRAVADO, TOTALEXENTO, TOTALVENTA,
                       TOTALDESCUENTOS, TOTALVENTANETA, TOTALCOMPROBANTE,
                       INFOREF_TIPODOC, INFOREF_NUMERO, INFOREF_FECHAEMISION,
                       INFOREF_CODIGO, INFOREF_RAZON, NORMAVIGENTE_NUMRESOLUCION,
                       NORMAVIGENTE_FECHARESOLUCION, DOCUMENTO, ESTADO,
                       TOTALIMPUESTO, IMPRESO, ESTADO_CLIENTE,
                       OBSERVACION, INFOREF_DOC, SUCURSAL,
                       DOCCREDITO, MENSAJE_HACIENDA, ESTADO_HACIENDA,
                       COD_RUTA, DOCDEBITO, REGENERADO,
                       REIMPRIME, COD_CLIENTE, TOTALSERVEXONERADO,
                       TOTALMERCEXONERADA, TOTALEXONERADO, TOTALIVADEVUELTO,
                       TOTALOTROSCARGOS, ENVIO_CORREO, COD_PROVEEDOR,
                       TIPO_DOC, FORMATO, EXPORTADO,
                       TRAMITAFACTURA, PROVEEDOR_SISTEMAS, TOTALSERVNOSUJETO,
                       TOTALMERCNOSUJETA, TOTALNOSUJETO, RECEPTOR_COD_ACTIVIDAD,
                       EMISOR_COD_ACTIVIDAD, ID_FACTURA_ORACLE, NUM_FACTURA_ORACLE, TRASLADO,
                       TRANSPORTISTA, MEDIOSPAGO,
                       PAIS_ORIGEN, FECHA_VENCIMIENTO, ORDEN_COMPRA,
                       AGENTE, OBSERVACIONES, FORMULARIO,
                       PESOBRUTO, PESONETO, LOTE,
                       BULTOS_PK, PESOBRUTO_KG, PESONETO_KG,
                       VENDEDOR_WALMART, TIENDA_WALMART, TERMINO_ENVIO, TARIMAS, MOTIVO_NC, NUMERO_BOLETA,
                       NUMERO_LINEA_FACTURA, NUMERO_INFORME_GASTO
                FROM VENDOCENCFED
                WHERE NVL(INDTRASLADO,'N') = 'N'
                  AND TIPODOC IS NOT NULL
                  AND CIA = :pCIA
                  AND NUM_FACTURA_ORACLE IN (
                      SELECT num FROM (
                          SELECT num, ROW_NUMBER() OVER (ORDER BY ruta, num) AS rn
                          FROM (
                              SELECT TRIM(NUM_FACTURA_ORACLE) AS num, MIN(COD_RUTA) AS ruta
                              FROM VENDOCENCFED
                              WHERE NVL(INDTRASLADO,'N') = 'N'
                                AND CIA = :pCIA
                                AND TIPODOC IS NOT NULL
                                AND NUM_FACTURA_ORACLE IS NOT NULL
                              GROUP BY TRIM(NUM_FACTURA_ORACLE)
                              HAVING COUNT(*) = 1
                          )
                      ) WHERE rn <= :pMax
                  )
                ORDER BY COD_RUTA ASC";

            var dt = new DataTable();

            // REVISION v5: la ventana que se lee de ATP = lote + facturas en revisión.
            // Así, aunque las trabadas salgan primero en el ORDER BY, siempre quedan
            // al menos "maxCiclo" facturas nuevas dentro de la ventana.
            int ciclo = Interlocked.Increment(ref _cicloActual);
            int maxCiclo = Math.Max(1, Math.Min(500, _opciones.MaxFacturasPorCiclo <= 0 ? 10 : _opciones.MaxFacturasPorCiclo));
            int ventana = Math.Min(500, maxCiclo + _enRevision.Count);

            if (maxCiclo + _enRevision.Count > 500)
                _log($"ADVERTENCIA: {_enRevision.Count} facturas en revisión. Pueden ocupar la ventana de lectura; revisarlas en el log.");

            using (var conSrc = OpenSrc())
            using (var cmd = conSrc.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.InitialLOBFetchSize = -1;
                cmd.CommandText = selectBase;
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                cmd.Parameters.Add(":pMax", OracleDbType.Int32, ventana, ParameterDirection.Input);

                using (var da = new OracleDataAdapter(cmd))
                    da.Fill(dt);
            }

            // REVISION v5: los números repetidos en ATP los excluye el SELECT (HAVING COUNT(*) = 1),
            // así que nunca llegan al log. Se cuentan aparte para que se vean.
            RegistrarRepetidosExcluidos();

            if (dt.Rows.Count == 0)
            {
                _enRevision.Clear();
                _log("No hay registros pendientes INDTRASLADO='N'.");
                return 0;
            }

            _log($"ATP cabeceras leídas: {dt.Rows.Count}");

            // Se agrupa y se arma la tabla de cada encabezado ANTES del paralelo
            // (el DataTable original no se toca desde varios hilos).
            var grupos = new List<GrupoFactura>();

            foreach (var g in dt.Rows
                .Cast<DataRow>()
                .Where(r => !string.IsNullOrWhiteSpace(r["NUM_FACTURA_ORACLE"]?.ToString()))
                .GroupBy(r => r["NUM_FACTURA_ORACLE"].ToString().Trim()))
            {
                var filas = g.ToList();
                var cab = dt.Clone();
                cab.ImportRow(filas[0]);

                grupos.Add(new GrupoFactura
                {
                    NumFacturaOracle = g.Key,
                    Filas = filas,
                    Cabecera = cab
                });
            }

            // ARREGLO 8 + REVISION v5: armar el lote del ciclo.
            //  1. Si la lectura trajo menos que la ventana, se vio TODO lo pendiente:
            //     se olvidan las facturas en revisión que ya no están pendientes.
            //  2. Las que están en revisión y todavía no les toca reintento se saltan.
            //  3. Primero las nuevas, después las en revisión a las que ya les toca
            //     (OrderBy es estable: respeta el ORDER BY del SELECT).
            int totalLeidas = grupos.Count;

            if (totalLeidas < ventana)
            {
                var vistas = new HashSet<string>(grupos.Select(x => x.NumFacturaOracle));
                foreach (var num in _enRevision.Keys.Where(k => !vistas.Contains(k)).ToList())
                {
                    int ignorar;
                    _enRevision.TryRemove(num, out ignorar);
                }
            }

            int enDescanso = grupos.Count(x => EnDescanso(x.NumFacturaOracle, ciclo));

            grupos = grupos
                .Where(x => !EnDescanso(x.NumFacturaOracle, ciclo))
                .OrderBy(x => _enRevision.ContainsKey(x.NumFacturaOracle) ? 1 : 0)
                .Take(maxCiclo)
                .ToList();

            _log($"Ciclo {ciclo} | Leídas de ATP: {totalLeidas} | En revisión: {_enRevision.Count} (esperando reintento: {enDescanso}) | Se procesan en este ciclo: {grupos.Count}");

            int ok = 0, pendientes = 0, errores = 0;
            var lockObj = new object();
            var po = new ParallelOptions { MaxDegreeOfParallelism = _maxDegree };

            Parallel.ForEach(grupos, po, (g, estado) =>
            {
                // ARREGLO 8: si el servicio se está deteniendo, no se empiezan facturas nuevas.
                // Las que ya están en curso terminan su transacción normalmente.
                if (_detener)
                {
                    estado.Stop();
                    return;
                }

                ResultadoTraslado r;

                try
                {
                    r = TrasladarFacturaAtomica(g);
                }
                catch (Exception ex)
                {
                    _log($"ERROR NO CONTROLADO trasladando FacturaOracle={g.NumFacturaOracle} | {ex.Message}");
                    r = ResultadoTraslado.Error;
                }

                // REVISION v5: recordar las que no avanzaron, para que no tapen el lote.
                //  Pendiente (sin detalle todavía, montos, marca en ATP): se reintenta el siguiente ciclo.
                //  Error (requiere revisión): descansa CiclosReintentoRevision ciclos.
                int ignorar;
                if (r == ResultadoTraslado.Ok)
                    _enRevision.TryRemove(g.NumFacturaOracle, out ignorar);
                else if (r == ResultadoTraslado.Pendiente)
                    _enRevision[g.NumFacturaOracle] = ciclo + 1;
                else
                    _enRevision[g.NumFacturaOracle] = ciclo + Math.Max(1, _opciones.CiclosReintentoRevision);

                lock (lockObj)
                {
                    if (r == ResultadoTraslado.Ok) ok++;
                    else if (r == ResultadoTraslado.Pendiente) pendientes++;
                    else errores++;
                }
            });

            _log($"Resumen traslado: Exitosas={ok} | En espera={pendientes} | Error={errores}");

            return ok;
        }

        private ResultadoTraslado TrasladarFacturaAtomica(GrupoFactura g)
        {
            string numFacturaOracle = g.NumFacturaOracle;
            DataRow cab = g.Cabecera.Rows[0];

            var factura = new FacturaInfo
            {
                NumFacturaOracle = numFacturaOracle,
                Documento = cab["DOCUMENTO"]?.ToString()?.Trim(),
                Clave = cab["CLAVE"]?.ToString()?.Trim()
            };

            string tipoDoc = cab["TIPODOC"]?.ToString()?.Trim();
            string idOracle = cab["ID_FACTURA_ORACLE"]?.ToString()?.Trim();
            string ctx = $"Documento={factura.Documento} | FacturaOracle={numFacturaOracle} | TipoDoc={tipoDoc}";

            // ---------------------------------------------------------------
            // ARREGLO 3: más de un encabezado pendiente con el mismo número
            // ---------------------------------------------------------------
            if (g.Filas.Count > 1)
            {
                string err = $"NUM_FACTURA_ORACLE repetido en ATP ({g.Filas.Count} encabezados pendientes). Revisar manualmente.";
                _log($"OMITIDA | {ctx} | {err}");
                _log($"REQUIERE REVISION SIN MODIFICAR ATP | {ctx} | {err}");
                return ResultadoTraslado.Error;
            }

            // REVISION v5: el detalle de ATP se lee ANTES de revisar el staging, porque
            // hace falta para conciliar una factura que ya está en 11g (ver abajo).
            DataTable dtDet = LeerAtp(SqlDetalleAtp, numFacturaOracle, tipoDoc);
            DataTable dtImp = LeerAtp(SqlImpuestoAtp, numFacturaOracle, null);

            // ---------------------------------------------------------------
            // ¿Ya existe en el staging de 11g?
            // ---------------------------------------------------------------
            using (var con = OpenDst())
            {
                string idEnDestino;
                int cabEnDestino = ContarCabeceraEnDestino(con, numFacturaOracle, out idEnDestino);

                if (cabEnDestino > 0)
                {
                    bool mismaFactura = !string.IsNullOrEmpty(idOracle) && idOracle == idEnDestino;
                    int detEnDestino = ContarEnDestino(con, "XXORA_VENDOCDETFED", numFacturaOracle);

                    // PROTECCION: un detalle existente NO demuestra que la factura este completa.
                    // REVISION v5: se concilia solo si la copia en 11g es IGUAL a ATP:
                    // 1 encabezado, misma cantidad de líneas de detalle e impuesto y misma
                    // suma de MONTOTOTALLINEA. Es el caso típico de "se trasladó bien pero
                    // falló marcar 'S' en ATP". Cualquier diferencia queda para revisión manual.
                    if (mismaFactura && detEnDestino > 0)
                    {
                        string diferencia = null;
                        if (cabEnDestino == 1 && CopiaIgualEnDestino(con, numFacturaOracle, dtDet, dtImp, out diferencia))
                        {
                            _log($"CONCILIADA: la copia en 11g es igual a ATP, se marca 'S' | {ctx}");
                            return MarcarTrasladada(numFacturaOracle) ? ResultadoTraslado.Ok : ResultadoTraslado.Pendiente;
                        }

                        string advertencia = "Cabecera preexistente con copia distinta a ATP: " +
                            (cabEnDestino == 1 ? diferencia : $"{cabEnDestino} encabezados en 11g");
                        _log($"REVISION MANUAL | {ctx} | {advertencia}");
                        return ResultadoTraslado.Error;
                    }

                    string err = mismaFactura
                        ? "Encabezado sin detalle en XXORA_VENDOCENCFED (corrida anterior interrumpida). Revisar antes de reenviar."
                        : $"NUM_FACTURA_ORACLE ya existe en XXORA_VENDOCENCFED (ID_FACTURA_ORACLE destino={idEnDestino}). Documento repetido.";

                    _log($"ERROR | {ctx} | {err}");
                    _log($"REQUIERE REVISION SIN MODIFICAR ATP | {ctx} | {err}");
                    return ResultadoTraslado.Error;
                }
            }

            // ---------------------------------------------------------------
            // ARREGLO 2: validar antes de trasladar
            // ---------------------------------------------------------------
            if (dtDet.Rows.Count == 0)
                return EsperarOMarcarError(factura, "Encabezado en ATP sin líneas de detalle", ctx);

            // Validación estructural: no transferir detalle con LINEADETALLE nula o repetida.
            var clavesDetalle = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow fila in dtDet.Rows)
            {
                string linea = fila["LINEADETALLE"] == DBNull.Value ? null : Convert.ToString(fila["LINEADETALLE"], CultureInfo.InvariantCulture).Trim();
                if (string.IsNullOrEmpty(linea) || !clavesDetalle.Add(linea))
                {
                    // REVISION v5: Error (y no Pendiente) para que descanse y no ocupe el lote:
                    // es un dato de ATP que no se corrige solo.
                    _log($"DETALLE INCONSISTENTE | {ctx} | LINEADETALLE nula o duplicada={linea} | Próximo reintento en {_opciones.CiclosReintentoRevision} ciclos");
                    return ResultadoTraslado.Error;
                }
            }
            var clavesImpuesto = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (DataRow fila in dtImp.Rows)
            {
                string linea = Convert.ToString(fila["LINEADETALLE"], CultureInfo.InvariantCulture)?.Trim();
                string codigo = Convert.ToString(fila["CODIGO"], CultureInfo.InvariantCulture)?.Trim() ?? "";
                string tarifa = Convert.ToString(fila["CODIGOTARIFA"], CultureInfo.InvariantCulture)?.Trim() ?? "";
                if (string.IsNullOrEmpty(linea) || !clavesDetalle.Contains(linea) ||
                    !clavesImpuesto.Add(linea + "|" + codigo + "|" + tarifa))
                {
                    _log($"IMPUESTO INCONSISTENTE | {ctx} | LINEADETALLE={linea} CODIGO={codigo} CODIGOTARIFA={tarifa} | Próximo reintento en {_opciones.CiclosReintentoRevision} ciclos");
                    return ResultadoTraslado.Error;
                }
            }

            decimal totalCab = Math.Abs(
                ToDec(cab["TOTALCOMPROBANTE"])
                - ToDec(cab["TOTALOTROSCARGOS"])
                + ToDec(cab["TOTALIVADEVUELTO"]));

            decimal totalDet = Math.Abs(dtDet.Rows.Cast<DataRow>().Sum(r => ToDec(r["MONTOTOTALLINEA"])));

            if (_opciones.ValidarMontos && Math.Abs(totalCab - totalDet) > _opciones.ToleranciaMonto)
            {
                return EsperarOMarcarError(
                    factura,
                    $"Montos no cuadran: encabezado={totalCab} vs suma de líneas={totalDet} ({dtDet.Rows.Count} líneas)",
                    ctx);
            }

            // ---------------------------------------------------------------
            // ARREGLO 1: una factura = una transacción en 11g
            // ---------------------------------------------------------------
            using (var con = OpenDst())
            using (var tx = con.BeginTransaction(IsolationLevel.ReadCommitted))
            {
                try
                {
                    // ARREGLO 7 (decisión del equipo): si en staging quedaron líneas de esta
                    // factura SIN encabezado, NO se borran automáticamente; se deja para
                    // conciliación manual (consulta 6 de consultas_previa_instalacion.sql).
                    int detHuerfano = ContarEnDestino(con, "XXORA_VENDOCDETFED", numFacturaOracle);
                    int impHuerfano = ContarEnDestino(con, "XXORA_VENDOCDETIMPFED", numFacturaOracle);
                    if (detHuerfano + impHuerfano > 0)
                    {
                        tx.Rollback();
                        _log($"LINEAS HUERFANAS EN 11g, requiere conciliación manual | {ctx} | Detalle={detHuerfano} | Impuesto={impHuerfano} | Próximo reintento en {_opciones.CiclosReintentoRevision} ciclos");
                        return ResultadoTraslado.Error;
                    }

                    // El encabezado va de último: GuardarFacturas nunca lo ve sin su detalle.
                    InsertarArrayBind(con, dtDet, "XXORA_VENDOCDETFED", $"Detalle | {ctx}");
                    InsertarArrayBind(con, dtImp, "XXORA_VENDOCDETIMPFED", $"Impuesto | {ctx}");
                    InsertarArrayBind(con, g.Cabecera, "XXORA_VENDOCENCFED", $"Encabezado | {ctx}");

                    // ARREGLO 7: antes del COMMIT, en staging debe estar exactamente lo que se
                    // insertó (1 encabezado y las mismas líneas). Si otro proceso metió la misma
                    // factura en paralelo, el conteo no cuadra y se revierte todo.
                    int cabFinal = ContarEnDestino(con, "XXORA_VENDOCENCFED", numFacturaOracle);
                    int detFinal = ContarEnDestino(con, "XXORA_VENDOCDETFED", numFacturaOracle);
                    int impFinal = ContarEnDestino(con, "XXORA_VENDOCDETIMPFED", numFacturaOracle);

                    if (cabFinal != 1 || detFinal != dtDet.Rows.Count || impFinal != dtImp.Rows.Count)
                    {
                        throw new Exception(
                            $"Conteo en staging no coincide (posible duplicado): " +
                            $"encabezados={cabFinal}/1, detalle={detFinal}/{dtDet.Rows.Count}, impuesto={impFinal}/{dtImp.Rows.Count}");
                    }

                    tx.Commit();
                }
                catch (Exception ex)
                {
                    try { tx.Rollback(); } catch { /* si la conexión cayó, Oracle ya hizo rollback */ }

                    string err = $"Traslado revertido (rollback) | {ex.Message}";
                    _log($"ERROR | {ctx} | {err} | ATP queda en 'N' | Próximo reintento en {_opciones.CiclosReintentoRevision} ciclos");
                    return ResultadoTraslado.Error;
                }
            }

            int ignorado;
            _ciclosEspera.TryRemove(numFacturaOracle, out ignorado);

            _log($"Trasladada OK | {ctx} | LineasDetalle={dtDet.Rows.Count} | LineasImpuesto={dtImp.Rows.Count}");

            // Si este UPDATE falla, la factura ya está completa en 11g: el siguiente ciclo
            // la encuentra en staging y la concilia (CONCILIADA) si la copia es igual a ATP.
            return MarcarTrasladada(numFacturaOracle) ? ResultadoTraslado.Ok : ResultadoTraslado.Pendiente;
        }

        // REVISION v5: compara la copia que ya está en 11g contra lo que hay en ATP.
        private bool CopiaIgualEnDestino(OracleConnection con, string numFacturaOracle,
                                         DataTable dtDet, DataTable dtImp, out string diferencia)
        {
            diferencia = null;

            int detDestino;
            decimal sumaDestino;

            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = @"
                    SELECT COUNT(1) AS CANT, NVL(SUM(MONTOTOTALLINEA), 0) AS SUMA
                      FROM XXORA_VENDOCDETFED
                     WHERE NUM_FACTURA_ORACLE = :p
                       AND CIA = :pCIA";
                cmd.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                using (var rd = cmd.ExecuteReader())
                {
                    rd.Read();
                    detDestino = Convert.ToInt32(rd["CANT"]);
                    sumaDestino = ToDec(rd["SUMA"]);
                }
            }

            int impDestino = ContarEnDestino(con, "XXORA_VENDOCDETIMPFED", numFacturaOracle);
            decimal sumaAtp = dtDet.Rows.Cast<DataRow>().Sum(r => ToDec(r["MONTOTOTALLINEA"]));

            if (detDestino != dtDet.Rows.Count)
                diferencia = $"líneas de detalle 11g={detDestino} vs ATP={dtDet.Rows.Count}";
            else if (impDestino != dtImp.Rows.Count)
                diferencia = $"líneas de impuesto 11g={impDestino} vs ATP={dtImp.Rows.Count}";
            else if (Math.Abs(sumaDestino - sumaAtp) > 0.01m)
                diferencia = $"suma MONTOTOTALLINEA 11g={sumaDestino} vs ATP={sumaAtp}";

            return diferencia == null;
        }

        // REVISION v5: true si la factura está en revisión y todavía no le toca reintento.
        private bool EnDescanso(string numFacturaOracle, int ciclo)
        {
            int proximo;
            return _enRevision.TryGetValue(numFacturaOracle, out proximo) && proximo > ciclo;
        }

        // REVISION v5: deja en el log cuántos números están repetidos entre los pendientes de ATP.
        // Esos no los toma el SELECT del traslado y hay que revisarlos a mano.
        private void RegistrarRepetidosExcluidos()
        {
            try
            {
                using (var con = OpenSrc())
                using (var cmd = con.CreateCommand())
                {
                    cmd.BindByName = true;
                    cmd.CommandText = @"
                        SELECT COUNT(*) AS NUMEROS, NVL(SUM(CANT), 0) AS FILAS
                          FROM (SELECT TRIM(NUM_FACTURA_ORACLE) AS NUM, COUNT(*) AS CANT
                                  FROM VENDOCENCFED
                                 WHERE NVL(INDTRASLADO,'N') = 'N'
                                   AND CIA = :pCIA
                                   AND TIPODOC IS NOT NULL
                                   AND NUM_FACTURA_ORACLE IS NOT NULL
                                 GROUP BY TRIM(NUM_FACTURA_ORACLE)
                                HAVING COUNT(*) > 1)";
                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    using (var rd = cmd.ExecuteReader())
                    {
                        if (rd.Read() && Convert.ToInt32(rd["NUMEROS"]) > 0)
                            _log($"REPETIDOS EN ATP SIN TRASLADAR (revisión manual) | Números={rd["NUMEROS"]} | Filas={rd["FILAS"]}");
                    }
                }
            }
            catch (Exception ex)
            {
                _log($"No se pudo contar repetidos en ATP | {ex.Message}");
            }
        }

        private ResultadoTraslado EsperarOMarcarError(FacturaInfo factura, string motivo, string ctx)
        {
            int ciclos = _ciclosEspera.AddOrUpdate(factura.NumFacturaOracle, 1, (k, v) => v + 1);

            if (ciclos < _opciones.MaxCiclosEspera)
            {
                _log($"EN ESPERA ({ciclos}/{_opciones.MaxCiclosEspera}) | {ctx} | {motivo}");
                return ResultadoTraslado.Pendiente;
            }

            // El origen puede tardar más que MaxCiclosEspera; nunca bloquearlo permanentemente.
            // Sin una tabla persistente de reintentos, mantener N y registrar el diagnóstico.
            // REVISION v5: sigue en 'N' (no se toca ATP), pero se devuelve Error para que
            // descanse CiclosReintentoRevision ciclos y no se reintente en cada ciclo.
            _log($"PENDIENTE PROLONGADO ({ciclos} ciclos) | {ctx} | {motivo} | Próximo reintento en {_opciones.CiclosReintentoRevision} ciclos");
            return ResultadoTraslado.Error;
        }

        private DataTable LeerAtp(string sql, string numFacturaOracle, string tipoDoc)
        {
            var dt = new DataTable();

            using (var con = OpenSrc())
            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.InitialLOBFetchSize = -1;
                cmd.CommandText = sql;
                cmd.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                if (sql.Contains(":pTIPO"))
                    cmd.Parameters.Add(":pTIPO", OracleDbType.Varchar2, tipoDoc, ParameterDirection.Input);

                using (var da = new OracleDataAdapter(cmd))
                    da.Fill(dt);
            }

            return dt;
        }

        private int ContarCabeceraEnDestino(OracleConnection con, string numFacturaOracle, out string idFacturaOracle)
        {
            idFacturaOracle = null;

            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = @"
                    SELECT COUNT(1) AS CANT, MAX(ID_FACTURA_ORACLE) AS ID_ORACLE
                      FROM XXORA_VENDOCENCFED
                     WHERE NUM_FACTURA_ORACLE = :p
                       AND CIA = :pCIA";
                cmd.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                using (var rd = cmd.ExecuteReader())
                {
                    if (!rd.Read())
                        return 0;

                    idFacturaOracle = rd["ID_ORACLE"] == DBNull.Value ? null : rd["ID_ORACLE"].ToString().Trim();
                    return Convert.ToInt32(rd["CANT"]);
                }
            }
        }

        private int ContarEnDestino(OracleConnection con, string tabla, string numFacturaOracle)
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = $"SELECT COUNT(1) FROM {tabla} WHERE NUM_FACTURA_ORACLE = :p AND CIA = :pCIA";
                cmd.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                return Convert.ToInt32(cmd.ExecuteScalar());
            }
        }

        // ARREGLO 7: borra líneas de staging de esta factura solo si NO tienen encabezado.
        private int BorrarHuerfanosEnDestino(OracleConnection con, string tabla, string numFacturaOracle)
        {
            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText =
                    $"DELETE FROM {tabla} " +
                    " WHERE NUM_FACTURA_ORACLE = :p AND CIA = :pCIA " +
                    "   AND NOT EXISTS (SELECT 1 FROM XXORA_VENDOCENCFED e " +
                    "                    WHERE e.NUM_FACTURA_ORACLE = :p AND e.CIA = :pCIA)";
                cmd.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                return cmd.ExecuteNonQuery();
            }
        }

        private bool MarcarTrasladada(string numFacturaOracle)
        {
            try
            {
                using (var con = OpenSrc())
                using (var cmd = con.CreateCommand())
                {
                    cmd.BindByName = true;
                    cmd.CommandText = @"
                        UPDATE VENDOCENCFED
                           SET INDTRASLADO = 'S'
                         WHERE NUM_FACTURA_ORACLE = :p
                           AND CIA = :pCIA
                           AND NVL(INDTRASLADO,'N') = 'N'";
                    cmd.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                    using (var tx = con.BeginTransaction())
                    {
                        using (var check = con.CreateCommand())
                        {
                            check.BindByName = true;
                            check.CommandText = @"SELECT COUNT(*) FROM VENDOCENCFED
                                                   WHERE NUM_FACTURA_ORACLE = :p AND CIA = :pCIA
                                                     AND NVL(INDTRASLADO,'N') = 'N'";
                            check.Parameters.Add(":p", OracleDbType.Varchar2, numFacturaOracle, ParameterDirection.Input);
                            check.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                            int candidatas = Convert.ToInt32(check.ExecuteScalar());
                            if (candidatas != 1)
                            {
                                tx.Rollback();
                                _log($"NO MARCADA EN ATP | FacturaOracle={numFacturaOracle} | Coincidencias pendientes={candidatas} (se requiere identidad unica)");
                                return false;
                            }
                        }
                        int actualizadas = cmd.ExecuteNonQuery();
                        if (actualizadas != 1)
                        {
                            tx.Rollback();
                            _log($"NO MARCADA EN ATP | FacturaOracle={numFacturaOracle} | Filas afectadas={actualizadas}");
                            return false;
                        }
                        tx.Commit();
                    }
                    return true;
                }
            }
            catch (Exception ex)
            {
                _log($"ERROR marcando INDTRASLADO='S' | FacturaOracle={numFacturaOracle} | {ex.Message} (requiere conciliación)");
                return false;
            }
        }

        // INSERT con array binding dentro de la transacción abierta en la conexión.
        // Reutiliza ObtenerColumnasDestino / PrepararDatosParaDestino: mismas validaciones
        // y mismo log de diagnóstico que usaba el bulk copy.
        private void InsertarArrayBind(OracleConnection con, DataTable datos, string tabla, string contexto)
        {
            if (datos == null || datos.Rows.Count == 0)
                return;

            var columnas = _cacheColumnas.GetOrAdd(tabla, t => ObtenerColumnasDestino(con, t));
            var prep = PrepararDatosParaDestino(con, datos, tabla, contexto, columnas);

            var nombres = prep.Columns.Cast<DataColumn>().Select(c => c.ColumnName).ToList();

            string sqlInsert =
                $"INSERT INTO {tabla} ({string.Join(", ", nombres)}) " +
                $"VALUES ({string.Join(", ", nombres.Select((n, i) => ":p" + i))})";

            // El array binding con columnas LOB/LONG no es confiable en todas las versiones
            // de ODP.NET. Si la tabla tiene alguna, o si es una sola fila (el encabezado),
            // se inserta fila por fila con parámetros normales.
            bool tieneLob = nombres.Any(n => EsTipoLob(columnas[n].DataType));

            if (tieneLob || prep.Rows.Count == 1)
            {
                using (var cmd = con.CreateCommand())
                {
                    cmd.BindByName = true;
                    cmd.CommandText = sqlInsert;

                    var parametros = nombres
                        .Select((n, i) => cmd.Parameters.Add(":p" + i, MapearTipoOracle(columnas[n])))
                        .ToList();

                    foreach (DataRow fila in prep.Rows)
                    {
                        for (int i = 0; i < parametros.Count; i++)
                            parametros[i].Value = fila[i];

                        cmd.ExecuteNonQuery();
                    }
                }

                return;
            }

            using (var cmd = con.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandText = sqlInsert;
                cmd.ArrayBindCount = prep.Rows.Count;

                for (int i = 0; i < nombres.Count; i++)
                {
                    int idx = i;
                    object[] valores = prep.Rows.Cast<DataRow>().Select(r => r[idx]).ToArray();

                    cmd.Parameters.Add(
                        ":p" + i,
                        MapearTipoOracle(columnas[nombres[i]]),
                        valores,
                        ParameterDirection.Input);
                }

                cmd.ExecuteNonQuery();
            }
        }

        private OracleDbType MapearTipoOracle(ColumnaDestinoInfo c)
        {
            string t = (c.DataType ?? "").ToUpperInvariant();

            if (t == "NUMBER" || t == "DECIMAL" || t == "NUMERIC" || t == "INTEGER") return OracleDbType.Decimal;
            // PrepararDatosParaDestino convierte FLOAT/BINARY_FLOAT/BINARY_DOUBLE a double
            if (t == "FLOAT" || t == "BINARY_DOUBLE" || t == "BINARY_FLOAT") return OracleDbType.Double;
            if (t == "LONG") return OracleDbType.Long;
            if (t == "DATE") return OracleDbType.Date;
            if (t.StartsWith("TIMESTAMP")) return OracleDbType.TimeStamp;
            if (t == "CLOB") return OracleDbType.Clob;
            if (t == "NCLOB") return OracleDbType.NClob;
            if (t == "NVARCHAR2" || t == "NCHAR") return OracleDbType.NVarchar2;
            if (t == "CHAR") return OracleDbType.Char;
            if (t == "RAW") return OracleDbType.Raw;
            if (t == "LONG RAW") return OracleDbType.LongRaw;
            if (t == "BLOB") return OracleDbType.Blob;
            return OracleDbType.Varchar2;
        }

        private static bool EsTipoLob(string dataType)
        {
            string t = (dataType ?? "").ToUpperInvariant();
            return t == "CLOB" || t == "NCLOB" || t == "BLOB" || t == "LONG" || t == "LONG RAW";
        }

        private decimal ToDec(object v)
        {
            if (v == null || v == DBNull.Value) return 0m;
            decimal d;
            if (!decimal.TryParse(Convert.ToString(v, CultureInfo.InvariantCulture),
                                  NumberStyles.Any, CultureInfo.InvariantCulture, out d))
                throw new FormatException("Valor monetario no numérico; se detiene traslado para evitar datos inválidos.");
            return d;
        }

        // REVISION v5: equivalencia de tipos entre ATP y 11g.
        // GuardarFacturas guarda como tiquete ('04') una factura ('01') sin receptor válido.
        private static string TipoEquivalenteEnAtp(string tipo11g)
        {
            return tipo11g == "04" ? "01" : tipo11g;
        }

        private static string TipoEquivalenteEn11g(string tipoAtp)
        {
            return tipoAtp == "01" ? "04" : tipoAtp;
        }

        public void EjecutarGuardarFacturas(int pLote)
        {
            using (var conDst = OpenDst())
            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "NUEVO.facturaElecOracle.GuardarFacturas";
                // REVISION v5: configurable (TimeoutGuardarFacturasSeg, 0 = sin límite).
                // Al vencer, ODP.NET cancela la llamada en la base (ORA-01013) y el paquete
                // revierte TODO el lote, con los consecutivos ya consumidos. Debe ser
                // bastante mayor que lo que tarda un lote normal.
                cmd.CommandTimeout = Math.Max(0, _opciones.TimeoutGuardarFacturasSeg);
                cmd.Parameters.Add("p_lote", OracleDbType.Int32, Math.Max(1, pLote), ParameterDirection.Input);
                cmd.ExecuteNonQuery();
            }

            _log("GuardarFacturas ejecutado.");
        }

        public int SincronizarEstadoHacienda_PorDocumento()
        {
            var dt = new DataTable();

            // ============================================================
            // 1. Leer documentos aceptados pendientes en Oracle 11g
            // ============================================================
            using (var conDst = OpenDst())
            using (var cmdSel = conDst.CreateCommand())
            {
                cmdSel.BindByName = true;
                cmdSel.CommandText = @"
                    SELECT CLAVE,
                           REPLACE(DOCUMENTO,'08-','') AS DOCUMENTO,
                           TIPODOC,
                           NUMEROCONSECUTIVO,
                           ESTADO,
                           MENSAJE_HACIENDA
                      FROM VENDOCENCFED
                     WHERE NVL(ACT_ORACLE,'N') = 'N'
                       AND NVL(INDORACLE,'N') = 'S'
                       AND ESTADO = 'A'
                       AND CIA = :pCIA";

                cmdSel.Parameters.Add(
                    ":pCIA",
                    OracleDbType.Varchar2,
                    _ciaFiltro,
                    ParameterDirection.Input);

                using (var da = new OracleDataAdapter(cmdSel))
                    da.Fill(dt);
            }

            if (dt.Rows.Count == 0)
            {
                _log("No hay registros por sincronizar.");
                return 0;
            }

            _log($"Documentos pendientes de sincronizar: {dt.Rows.Count}");

            int actualizados = 0;
            int omitidosDuplicados = 0;
            int noEncontrados = 0;
            int errores = 0;

            // ============================================================
            // 2. Procesar documento por documento.
            //    No usamos ArrayBindCount para que una CLAVE duplicada
            //    no detenga todo el lote con ORA-24381.
            // ============================================================
            foreach (DataRow r in dt.Rows)
            {
                string clave =
                    r["CLAVE"] == DBNull.Value
                        ? null
                        : r["CLAVE"].ToString().Trim();

                string documento =
                    r["DOCUMENTO"] == DBNull.Value
                        ? null
                        : r["DOCUMENTO"].ToString().Trim();

                string numeroConsecutivo =
                    r["NUMEROCONSECUTIVO"] == DBNull.Value
                        ? null
                        : r["NUMEROCONSECUTIVO"].ToString().Trim();

                string estado =
                    r["ESTADO"] == DBNull.Value
                        ? null
                        : r["ESTADO"].ToString().Trim();

                string mensaje =
                    r["MENSAJE_HACIENDA"] == DBNull.Value
                        ? null
                        : r["MENSAJE_HACIENDA"].ToString();

                // ARREGLO 5: el tipo de documento identifica la fila exacta en ATP
                string tipoDoc =
                    r["TIPODOC"] == DBNull.Value
                        ? null
                        : r["TIPODOC"].ToString().Trim();

                try
                {
                    using (var conSrc = OpenSrc())
                    {
                        string rowIdDocumento = null;
                        string claveActual = null;

                        // ====================================================
                        // 3. Buscar la fila exacta del documento en ATP
                        // ====================================================
                        int filasAtp = 0;

                        using (var cmdBuscar = conSrc.CreateCommand())
                        {
                            cmdBuscar.BindByName = true;
                            // ARREGLO 5: número + tipo de documento, sin ROWNUM = 1
                            // REVISION v5: GuardarFacturas convierte una factura 01 en tiquete 04
                            // cuando no hay receptor válido. En 11g queda '04' y en ATP sigue '01';
                            // por eso un '04' de 11g se busca en ATP como '04' o '01'.
                            cmdBuscar.CommandText = @"
                                SELECT ROWIDTOCHAR(ROWID) AS RID,
                                       CLAVE
                                  FROM VENDOCENCFED
                                 WHERE NUM_FACTURA_ORACLE = :pDOCUMENTO
                                   AND TRIM(TIPODOC) IN (:pTIPODOC, :pTIPOALT)
                                   AND CIA = :pCIA";

                            cmdBuscar.Parameters.Add(
                                ":pDOCUMENTO",
                                OracleDbType.Varchar2,
                                documento,
                                ParameterDirection.Input);

                            cmdBuscar.Parameters.Add(
                                ":pTIPODOC",
                                OracleDbType.Varchar2,
                                tipoDoc,
                                ParameterDirection.Input);

                            cmdBuscar.Parameters.Add(
                                ":pTIPOALT",
                                OracleDbType.Varchar2,
                                TipoEquivalenteEnAtp(tipoDoc),
                                ParameterDirection.Input);

                            cmdBuscar.Parameters.Add(
                                ":pCIA",
                                OracleDbType.Varchar2,
                                _ciaFiltro,
                                ParameterDirection.Input);

                            using (var rd = cmdBuscar.ExecuteReader())
                            {
                                while (rd.Read())
                                {
                                    filasAtp++;

                                    rowIdDocumento =
                                        rd["RID"] == DBNull.Value
                                            ? null
                                            : rd["RID"].ToString();

                                    claveActual =
                                        rd["CLAVE"] == DBNull.Value
                                            ? null
                                            : rd["CLAVE"].ToString().Trim();
                                }
                            }
                        }

                        // ARREGLO 5: si hay más de una fila no se adivina cuál es
                        if (filasAtp > 1)
                        {
                            errores++;

                            _log(
                                $"ATP CON {filasAtp} FILAS PARA EL MISMO DOCUMENTO, NO SE ACTUALIZA | " +
                                $"Documento={documento} | " +
                                $"TipoDoc={tipoDoc} | " +
                                $"Clave={clave}");

                            continue;
                        }

                        if (string.IsNullOrWhiteSpace(rowIdDocumento))
                        {
                            noEncontrados++;

                            _log(
                                $"ATP NO ENCONTRADO | " +
                                $"Documento={documento} | " +
                                $"Clave={clave}");

                            continue;
                        }

                        // ====================================================
                        // 4. Validar que la CLAVE no pertenezca a OTRA fila
                        // ====================================================
                        bool claveDuplicada = false;
                        string documentoQueTieneClave = null;
                        string facturaOracleQueTieneClave = null;

                        if (!string.IsNullOrWhiteSpace(clave))
                        {
                            using (var cmdDup = conSrc.CreateCommand())
                            {
                                cmdDup.BindByName = true;
                                cmdDup.CommandText = @"
                                    SELECT DOCUMENTO,
                                           NUM_FACTURA_ORACLE
                                      FROM VENDOCENCFED
                                     WHERE CLAVE = :pCLAVE
                                       AND ROWID <> CHARTOROWID(:pROWID)
                                       AND ROWNUM = 1";

                                cmdDup.Parameters.Add(
                                    ":pCLAVE",
                                    OracleDbType.Varchar2,
                                    clave,
                                    ParameterDirection.Input);

                                cmdDup.Parameters.Add(
                                    ":pROWID",
                                    OracleDbType.Varchar2,
                                    rowIdDocumento,
                                    ParameterDirection.Input);

                                using (var rd = cmdDup.ExecuteReader())
                                {
                                    if (rd.Read())
                                    {
                                        claveDuplicada = true;

                                        documentoQueTieneClave =
                                            rd["DOCUMENTO"] == DBNull.Value
                                                ? null
                                                : rd["DOCUMENTO"].ToString();

                                        facturaOracleQueTieneClave =
                                            rd["NUM_FACTURA_ORACLE"] == DBNull.Value
                                                ? null
                                                : rd["NUM_FACTURA_ORACLE"].ToString();
                                    }
                                }
                            }
                        }

                        // ====================================================
                        // 5. Si la clave ya está en otra fila, omitir solamente
                        //    ese documento. No marcar ACT_ORACLE='S'.
                        // ====================================================
                        if (claveDuplicada)
                        {
                            omitidosDuplicados++;

                            _log("===========================================");
                            _log("CLAVE DUPLICADA - DOCUMENTO OMITIDO");
                            _log($"Documento a actualizar : {documento}");
                            _log($"Clave Hacienda          : {clave}");
                            _log($"Clave actual ATP        : {claveActual}");
                            _log($"La clave ya pertenece a : {documentoQueTieneClave}");
                            _log($"Factura Oracle existente: {facturaOracleQueTieneClave}");
                            _log("No se actualiza ATP y NO se marca ACT_ORACLE='S'.");
                            _log("===========================================");

                            continue;
                        }

                        // ====================================================
                        // 6. Actualizar ATP
                        // ====================================================
                        int afectadosSrc;

                        using (var cmdUpd = conSrc.CreateCommand())
                        {
                            cmdUpd.BindByName = true;
                            cmdUpd.CommandText = @"
                                UPDATE VENDOCENCFED
                                   SET NUMEROCONSECUTIVO = :pNUMEROCONSECUTIVO,
                                       ESTADO            = :pESTADO,
                                       MENSAJE_HACIENDA  = :pMENSAJE_HACIENDA,
                                       CLAVE             = :pCLAVE
                                 WHERE ROWID = CHARTOROWID(:pROWID)";

                            cmdUpd.Parameters.Add(
                                ":pNUMEROCONSECUTIVO",
                                OracleDbType.Varchar2,
                                (object)numeroConsecutivo ?? DBNull.Value,
                                ParameterDirection.Input);

                            cmdUpd.Parameters.Add(
                                ":pESTADO",
                                OracleDbType.Varchar2,
                                (object)estado ?? DBNull.Value,
                                ParameterDirection.Input);

                            cmdUpd.Parameters.Add(
                                ":pMENSAJE_HACIENDA",
                                OracleDbType.Varchar2,
                                (object)mensaje ?? DBNull.Value,
                                ParameterDirection.Input);

                            cmdUpd.Parameters.Add(
                                ":pCLAVE",
                                OracleDbType.Varchar2,
                                (object)clave ?? DBNull.Value,
                                ParameterDirection.Input);

                            cmdUpd.Parameters.Add(
                                ":pROWID",
                                OracleDbType.Varchar2,
                                rowIdDocumento,
                                ParameterDirection.Input);

                            afectadosSrc = cmdUpd.ExecuteNonQuery();
                        }

                        if (afectadosSrc == 0)
                        {
                            _log(
                                $"ATP NO ACTUALIZADO | " +
                                $"Documento={documento} | " +
                                $"Clave={clave}");

                            continue;
                        }

                        // ====================================================
                        // 7. Solo después del UPDATE exitoso en ATP,
                        //    marcar el documento como sincronizado en 11g.
                        // ====================================================
                        using (var conDst = OpenDst())
                        using (var cmdUpdDst = conDst.CreateCommand())
                        {
                            cmdUpdDst.BindByName = true;
                            cmdUpdDst.CommandText = @"
                                UPDATE VENDOCENCFED
                                   SET ACT_ORACLE = 'S'
                                 WHERE REPLACE(DOCUMENTO,'08-','') = :pDOCUMENTO
                                   AND TRIM(TIPODOC) = :pTIPODOC
                                   AND CIA = :pCIA";  // ARREGLO 5

                            cmdUpdDst.Parameters.Add(
                                ":pDOCUMENTO",
                                OracleDbType.Varchar2,
                                documento,
                                ParameterDirection.Input);

                            cmdUpdDst.Parameters.Add(
                                ":pCIA",
                                OracleDbType.Varchar2,
                                _ciaFiltro,
                                ParameterDirection.Input);

                            cmdUpdDst.Parameters.Add(
                                ":pTIPODOC",
                                OracleDbType.Varchar2,
                                tipoDoc,
                                ParameterDirection.Input);

                            int afectadosDst = cmdUpdDst.ExecuteNonQuery();

                            _log(
                                $"SINCRONIZADO OK | " +
                                $"Documento={documento} | " +
                                $"Clave={clave} | " +
                                $"ATP={afectadosSrc} | " +
                                $"11g={afectadosDst}");
                        }

                        actualizados++;
                    }
                }
                catch (OracleException ex)
                {
                    errores++;

                    _log("===========================================");
                    _log("ERROR ORACLE SINCRONIZANDO DOCUMENTO");
                    _log($"Documento : {documento}");
                    _log($"Clave     : {clave}");
                    _log($"Oracle    : {ex.Number}");
                    _log($"Mensaje   : {ex.Message}");
                    _log("===========================================");
                }
                catch (Exception ex)
                {
                    errores++;

                    _log("===========================================");
                    _log("ERROR SINCRONIZANDO DOCUMENTO");
                    _log($"Documento : {documento}");
                    _log($"Clave     : {clave}");
                    _log($"Mensaje   : {ex.Message}");
                    _log("===========================================");
                }
            }

            // ============================================================
            // 8. Resumen
            // ============================================================
            _log("===========================================");
            _log("RESUMEN SINCRONIZACION HACIENDA -> ATP");
            _log($"Pendientes iniciales : {dt.Rows.Count}");
            _log($"Actualizados OK       : {actualizados}");
            _log($"Claves duplicadas     : {omitidosDuplicados}");
            _log($"No encontrados ATP    : {noEncontrados}");
            _log($"Errores               : {errores}");
            _log("===========================================");

            return actualizados;
        }


        public void VerificarConexionesIniciales()
        {
            _log("Verificando conexiones a bases de datos...");

            try
            {
                using (var conSrc = OpenSrc())
                using (var cmd = conSrc.CreateCommand())
                {
                    cmd.CommandText = "SELECT USER || ' - ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') FROM DUAL";
                    string resultado = Convert.ToString(cmd.ExecuteScalar());

                    _log("Conexión ATP ORIGEN OK. Usuario/Fecha: " + resultado);
                }
            }
            catch (Exception ex)
            {
                _log("ERROR conexión ATP ORIGEN: " + ex.Message);
                throw;
            }

            try
            {
                using (var conDst = OpenDst())
                using (var cmd = conDst.CreateCommand())
                {
                    cmd.CommandText = "SELECT USER || ' - ' || TO_CHAR(SYSDATE,'DD/MM/YYYY HH24:MI:SS') FROM DUAL";
                    string resultado = Convert.ToString(cmd.ExecuteScalar());

                    _log("Conexión Oracle 11g DESTINO OK. Usuario/Fecha: " + resultado);
                }
            }
            catch (Exception ex)
            {
                _log("ERROR conexión Oracle 11g DESTINO: " + ex.Message);
                throw;
            }

            _log("Verificación de conexiones finalizada correctamente.");
        }


        public int SincronizarFacturasRechazadasSinClave()
        {
            try
            {
                _log("===========================================");
                _log("INICIO - Sincronizar facturas rechazadas sin clave");
                _log("===========================================");

                var facturasPendientes = new List<string>();

                // ============================================================
                // 1. Buscar en ATP facturas que todavía no tienen CLAVE
                // ============================================================
                using (var conSrc = OpenSrc())
                using (var cmd = conSrc.CreateCommand())
                {
                    cmd.BindByName = true;

                    cmd.CommandText = @"
                SELECT DISTINCT TRIM(NUM_FACTURA_ORACLE) AS NUM_FACTURA_ORACLE
                FROM VENDOCENCFED
                WHERE CLAVE IS NULL
                  AND TIPODOC <> '08'
                  AND NUM_FACTURA_ORACLE IS NOT NULL
                  AND NVL(ESTADO, 'X') <> 'R'
                  AND CIA = :pCIA";

                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    using (var rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            string numeroFactura =
                                rd["NUM_FACTURA_ORACLE"] == DBNull.Value
                                    ? null
                                    : rd["NUM_FACTURA_ORACLE"].ToString().Trim();

                            if (!string.IsNullOrWhiteSpace(numeroFactura))
                                facturasPendientes.Add(numeroFactura);
                        }
                    }
                }

                if (facturasPendientes.Count == 0)
                {
                    _log("No hay facturas sin clave pendientes de revisar.");
                    return 0;
                }

                _log(
                    $"Facturas ATP sin clave encontradas: {facturasPendientes.Count}"
                );

                // ============================================================
                // 2. Revisar cuáles están rechazadas en Oracle 11g
                // ============================================================
                var facturasRechazadas = new List<string>();

                using (var conDst = OpenDst())
                using (var cmd = conDst.CreateCommand())
                {
                    cmd.BindByName = true;

                    cmd.CommandText = @"
                SELECT COUNT(1)
                FROM VENDOCENCFED
                WHERE documento = :pNUM_FACTURA_ORACLE
                  AND ESTADO = 'R'
                  AND CIA = :pCIA";

                    var pNumero = cmd.Parameters.Add(
                        ":pNUM_FACTURA_ORACLE",
                        OracleDbType.Varchar2);

                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    foreach (string numeroFactura in facturasPendientes)
                    {
                        pNumero.Value = numeroFactura;

                        int cantidad = Convert.ToInt32(cmd.ExecuteScalar());

                        if (cantidad > 0)
                        {
                            facturasRechazadas.Add(numeroFactura);

                            _log(
                                $"Factura rechazada encontrada en 11g | " +
                                $"NUM_FACTURA_ORACLE={numeroFactura}"
                            );
                        }
                    }
                }

                if (facturasRechazadas.Count == 0)
                {
                    _log(
                        "Ninguna de las facturas sin clave está rechazada en Oracle 11g."
                    );

                    _log("===========================================");

                    return 0;
                }

                _log(
                    $"Facturas rechazadas encontradas en 11g: {facturasRechazadas.Count}"
                );

                // ============================================================
                // 3. Actualizar ESTADO='R' en ATP
                // ============================================================
                int totalActualizadas = 0;

                using (var conSrc = OpenSrc())
                using (var cmd = conSrc.CreateCommand())
                {
                    cmd.BindByName = true;

                    cmd.CommandText = @"
                UPDATE VENDOCENCFED
                   SET ESTADO = 'R'
                 WHERE NUM_FACTURA_ORACLE = :pNUM_FACTURA_ORACLE
                   AND CLAVE IS NULL
                   AND TIPODOC <> '08'
                   AND NVL(ESTADO, 'X') <> 'R'
                   AND CIA = :pCIA";

                    var pNumero = cmd.Parameters.Add(
                        ":pNUM_FACTURA_ORACLE",
                        OracleDbType.Varchar2);

                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    foreach (string numeroFactura in facturasRechazadas)
                    {
                        pNumero.Value = numeroFactura;

                        int afectados = cmd.ExecuteNonQuery();

                        totalActualizadas += afectados;

                        _log(
                            $"ATP actualizado como rechazado | " +
                            $"NUM_FACTURA_ORACLE={numeroFactura} | " +
                            $"Registros={afectados}"
                        );
                    }
                }

                _log("-------------------------------------------");
                _log(
                    $"FIN sincronización rechazados | " +
                    $"Revisadas={facturasPendientes.Count} | " +
                    $"Rechazadas11g={facturasRechazadas.Count} | " +
                    $"ActualizadasATP={totalActualizadas}"
                );
                _log("===========================================");

                return totalActualizadas;
            }
            catch (Exception ex)
            {
                _log("===========================================");
                _log("ERROR SINCRONIZANDO FACTURAS RECHAZADAS");
                _log($"Mensaje    : {ex.Message}");

                if (ex.InnerException != null)
                    _log($"Inner      : {ex.InnerException.Message}");

                _log($"StackTrace : {ex.StackTrace}");
                _log("===========================================");

                throw;
            }
        }


        public int SincronizarFacturasAceptadasSinClave()
        {
            try
            {
                _log("===========================================");
                _log("INICIO - Sincronizar facturas aceptadas sin clave");
                _log("===========================================");

                var facturasPendientes = new List<Tuple<string, string>>();  // ARREGLO 5: número + tipo

                // ============================================================
                // 1. Buscar en ATP facturas que todavía no tienen CLAVE
                // ============================================================
                using (var conSrc = OpenSrc())
                using (var cmd = conSrc.CreateCommand())
                {
                    cmd.BindByName = true;

                    cmd.CommandText = @"
                SELECT DISTINCT TRIM(NUM_FACTURA_ORACLE) AS NUM_FACTURA_ORACLE,
                       TRIM(TIPODOC) AS TIPODOC
                FROM VENDOCENCFED
                WHERE CLAVE IS NULL
                  AND TIPODOC <> '08'
                  AND NUM_FACTURA_ORACLE IS NOT NULL
                  AND NVL(ESTADO, 'X') <> 'R'
                  AND CIA = :pCIA";

                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    using (var rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            string numeroFactura =
                                rd["NUM_FACTURA_ORACLE"] == DBNull.Value
                                    ? null
                                    : rd["NUM_FACTURA_ORACLE"].ToString().Trim();

                            if (!string.IsNullOrWhiteSpace(numeroFactura))
                                facturasPendientes.Add(Tuple.Create(
                                    numeroFactura,
                                    rd["TIPODOC"] == DBNull.Value ? null : rd["TIPODOC"].ToString()));
                        }
                    }
                }

                if (facturasPendientes.Count == 0)
                {
                    _log("No hay facturas sin clave pendientes de revisar.");
                    return 0;
                }

                _log(
                    $"Facturas ATP sin clave encontradas: {facturasPendientes.Count}"
                );

                // ============================================================
                // 2. Revisar cada factura en Oracle 11g
                //    Si está ESTADO='A', traer información de Hacienda
                // ============================================================

                int totalActualizadas = 0;

                using (var conDst = OpenDst())
                using (var cmdSel = conDst.CreateCommand())
                using (var conSrc = OpenSrc())
                using (var cmdUpdSrc = conSrc.CreateCommand())
                {
                    // ========================================================
                    // SELECT ORACLE 11g
                    // ========================================================
                    cmdSel.BindByName = true;

                    // ARREGLO 5: filtra por tipo.
                    // REVISION v5: una factura 01 de ATP puede estar en 11g como tiquete 04
                    // (GuardarFacturas la convierte si no hay receptor válido). Se acepta
                    // el tipo equivalente y se prefiere el tipo exacto si existieran ambos.
                    cmdSel.CommandText = @"
                SELECT * FROM (
                    SELECT CLAVE,
                           NUMEROCONSECUTIVO,
                           ESTADO,
                           MENSAJE_HACIENDA
                    FROM VENDOCENCFED
                    WHERE DOCUMENTO = :pNUM_FACTURA_ORACLE
                      AND ESTADO = 'A'
                      AND TRIM(TIPODOC) IN (:pTIPODOC, :pTIPOALT)
                      AND CIA = :pCIA
                    ORDER BY CASE WHEN TRIM(TIPODOC) = :pTIPODOC THEN 0 ELSE 1 END
                ) WHERE ROWNUM = 1";

                    var pNumero11g = cmdSel.Parameters.Add(
                        ":pNUM_FACTURA_ORACLE",
                        OracleDbType.Varchar2
                    );

                    cmdSel.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                    var pTipo11g = cmdSel.Parameters.Add(":pTIPODOC", OracleDbType.Varchar2);
                    var pTipoAlt11g = cmdSel.Parameters.Add(":pTIPOALT", OracleDbType.Varchar2);

                    // ========================================================
                    // UPDATE ATP
                    // ========================================================
                    cmdUpdSrc.BindByName = true;

                    cmdUpdSrc.CommandText = @"
                UPDATE VENDOCENCFED
                   SET NUMEROCONSECUTIVO = :pNUMEROCONSECUTIVO,
                       ESTADO            = :pESTADO,
                       MENSAJE_HACIENDA  = :pMENSAJE_HACIENDA,
                       CLAVE             = :pCLAVE
                 WHERE NUM_FACTURA_ORACLE = :pNUM_FACTURA_ORACLE
                   AND CLAVE IS NULL
                   AND TIPODOC <> '08'
                   AND TRIM(TIPODOC) = :pTIPODOC
                   AND CIA = :pCIA";  // ARREGLO 5: filtra por tipo

                    var pNumeroConsecutivo =
                        cmdUpdSrc.Parameters.Add(
                            ":pNUMEROCONSECUTIVO",
                            OracleDbType.Varchar2
                        );

                    var pEstado =
                        cmdUpdSrc.Parameters.Add(
                            ":pESTADO",
                            OracleDbType.Varchar2
                        );

                    var pMensaje =
                        cmdUpdSrc.Parameters.Add(
                            ":pMENSAJE_HACIENDA",
                            OracleDbType.Varchar2
                        );

                    var pClave =
                        cmdUpdSrc.Parameters.Add(
                            ":pCLAVE",
                            OracleDbType.Varchar2
                        );

                    var pNumeroFactura =
                        cmdUpdSrc.Parameters.Add(
                            ":pNUM_FACTURA_ORACLE",
                            OracleDbType.Varchar2
                        );

                    cmdUpdSrc.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);
                    var pTipoAtp = cmdUpdSrc.Parameters.Add(":pTIPODOC", OracleDbType.Varchar2);

                    // ========================================================
                    // Procesar facturas
                    // ========================================================
                    foreach (var pendiente in facturasPendientes)
                    {
                        string numeroFactura = pendiente.Item1;
                        string tipoDoc = pendiente.Item2;

                        pNumero11g.Value = numeroFactura;
                        pTipo11g.Value = tipoDoc;
                        pTipoAlt11g.Value = TipoEquivalenteEn11g(tipoDoc);   // REVISION v5
                        pTipoAtp.Value = tipoDoc;

                        using (var rd = cmdSel.ExecuteReader())
                        {
                            if (!rd.Read())
                            {
                                // No está aceptada todavía
                                continue;
                            }

                            string clave =
                                rd["CLAVE"] == DBNull.Value
                                    ? null
                                    : rd["CLAVE"].ToString().Trim();

                            string numeroConsecutivo =
                                rd["NUMEROCONSECUTIVO"] == DBNull.Value
                                    ? null
                                    : rd["NUMEROCONSECUTIVO"].ToString().Trim();

                            string estado =
                                rd["ESTADO"] == DBNull.Value
                                    ? null
                                    : rd["ESTADO"].ToString().Trim();

                            string mensajeHacienda =
                                rd["MENSAJE_HACIENDA"] == DBNull.Value
                                    ? null
                                    : rd["MENSAJE_HACIENDA"].ToString();

                            // =================================================
                            // Validación importante:
                            // Si 11g dice A pero todavía no tiene clave,
                            // no actualizamos ATP todavía.
                            // =================================================
                            if (string.IsNullOrWhiteSpace(clave))
                            {
                                _log(
                                    $"Factura aceptada en 11g pero sin CLAVE | " +
                                    $"NUM_FACTURA_ORACLE={numeroFactura}"
                                );

                                continue;
                            }

                            // =================================================
                            // Actualizar ATP
                            // =================================================

                            pNumeroConsecutivo.Value =
                                string.IsNullOrWhiteSpace(numeroConsecutivo)
                                    ? (object)DBNull.Value
                                    : numeroConsecutivo;

                            pEstado.Value =
                                string.IsNullOrWhiteSpace(estado)
                                    ? (object)DBNull.Value
                                    : estado;

                            pMensaje.Value =
                                string.IsNullOrWhiteSpace(mensajeHacienda)
                                    ? (object)DBNull.Value
                                    : mensajeHacienda;

                            pClave.Value =
                                clave;

                            pNumeroFactura.Value =
                                numeroFactura;

                            // ARREGLO 5: se confirma solo si afectó exactamente una fila
                            int afectados;

                            using (var tx = conSrc.BeginTransaction())
                            {
                                afectados = cmdUpdSrc.ExecuteNonQuery();

                                if (afectados == 1)
                                {
                                    tx.Commit();
                                }
                                else
                                {
                                    tx.Rollback();

                                    _log(
                                        $"ATP NO ACTUALIZADO (afectaría {afectados} filas) | " +
                                        $"NUM_FACTURA_ORACLE={numeroFactura} | " +
                                        $"TIPODOC={tipoDoc} | " +
                                        $"CLAVE={clave}");

                                    continue;
                                }
                            }

                            totalActualizadas += afectados;

                            _log(
                                $"ATP actualizado como aceptado | " +
                                $"NUM_FACTURA_ORACLE={numeroFactura} | " +
                                $"ESTADO={estado} | " +
                                $"CLAVE={clave} | " +
                                $"NUMEROCONSECUTIVO={numeroConsecutivo} | " +
                                $"Registros={afectados}"
                            );
                        }
                    }
                }

                _log("-------------------------------------------");
                _log(
                    $"FIN sincronización aceptadas sin clave | " +
                    $"Revisadas={facturasPendientes.Count} | " +
                    $"ActualizadasATP={totalActualizadas}"
                );
                _log("===========================================");

                return totalActualizadas;
            }
            catch (Exception ex)
            {
                _log("===========================================");
                _log("ERROR SINCRONIZANDO FACTURAS ACEPTADAS SIN CLAVE");
                _log($"Mensaje    : {ex.Message}");

                if (ex.InnerException != null)
                    _log($"Inner      : {ex.InnerException.Message}");

                _log($"StackTrace : {ex.StackTrace}");
                _log("===========================================");

                throw;
            }
        }

        public int SincronizarFechaFacturaElectronica()
        {
            try
            {
                _log("===========================================");
                _log("INICIO - Sincronizar FECHA_FACTURAELECTRONICA POR BLOQUES");
                _log("===========================================");

                // ============================================================
                // Cantidad de documentos por bloque
                // ============================================================
                const int TAMANO_BLOQUE = 500;

                var facturasPendientes = new List<string>();

                // ============================================================
                // 1. Obtener todas las facturas pendientes desde ATP
                // ============================================================
                using (var conSrc = OpenSrc())
                using (var cmd = conSrc.CreateCommand())
                {
                    cmd.BindByName = true;

                    cmd.CommandText = @"
                SELECT DISTINCT TRIM(NUM_FACTURA_ORACLE)
                       AS NUM_FACTURA_ORACLE
                FROM VENDOCENCFED
                WHERE FECHA_FACTURAELECTRONICA IS NULL
                  AND TIPODOC IS NOT NULL
                  AND NUM_FACTURA_ORACLE IS NOT NULL 
                  AND  TIPODOC IN ('01','03','04','09')
                  AND CIA = :pCIA";

                    cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                    using (var rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            if (rd["NUM_FACTURA_ORACLE"] == DBNull.Value)
                                continue;

                            string numero =
                                rd["NUM_FACTURA_ORACLE"]
                                    .ToString()
                                    .Trim();

                            if (!string.IsNullOrWhiteSpace(numero))
                            {
                                facturasPendientes.Add(numero);
                            }
                        }
                    }
                }

                // ============================================================
                // No hay nada pendiente
                // ============================================================
                if (facturasPendientes.Count == 0)
                {
                    _log(
                        "No hay registros pendientes de " +
                        "FECHA_FACTURAELECTRONICA."
                    );

                    return 0;
                }

                _log(
                    $"Total pendientes ATP: {facturasPendientes.Count}"
                );

                int totalActualizadas = 0;
                int totalEncontradas11g = 0;
                int totalNoEncontradas = 0;
                int numeroBloque = 0;

                // ============================================================
                // Abrimos UNA conexión a 11g y UNA conexión a ATP
                // para todo el proceso
                // ============================================================
                using (var conDst = OpenDst())
                using (var conSrc = OpenSrc())
                {
                    // ========================================================
                    // Procesar bloques
                    // ========================================================
                    for (
                        int inicio = 0;
                        inicio < facturasPendientes.Count;
                        inicio += TAMANO_BLOQUE
                    )
                    {
                        numeroBloque++;

                        List<string> bloque =
                            facturasPendientes
                                .Skip(inicio)
                                .Take(TAMANO_BLOQUE)
                                .ToList();

                        _log("-------------------------------------------");

                        _log(
                            $"Procesando bloque {numeroBloque} | " +
                            $"Documentos={bloque.Count}"
                        );

                        // ====================================================
                        // 2. Construir SELECT masivo para Oracle 11g
                        //
                        // Buscamos:
                        // DOCUMENTO = 1385600
                        //
                        // o:
                        // DOCUMENTO = 08-1385600
                        //
                        // IMPORTANTE:
                        // No usamos REPLACE(DOCUMENTO...) porque puede impedir
                        // que Oracle aproveche un índice sobre DOCUMENTO.
                        // ====================================================

                        var parametrosNormales =
                            new List<string>();

                        var parametros08 =
                            new List<string>();

                        using (var cmdSel = conDst.CreateCommand())
                        {
                            cmdSel.BindByName = true;
                            cmdSel.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                            for (int i = 0; i < bloque.Count; i++)
                            {
                                string nombreNormal =
                                    ":pDOC" + i;

                                string nombre08 =
                                    ":pDOC08_" + i;

                                parametrosNormales.Add(nombreNormal);
                                parametros08.Add(nombre08);

                                cmdSel.Parameters.Add(
                                    nombreNormal,
                                    OracleDbType.Varchar2,
                                    bloque[i],
                                    ParameterDirection.Input
                                );

                                cmdSel.Parameters.Add(
                                    nombre08,
                                    OracleDbType.Varchar2,
                                    "08-" + bloque[i],
                                    ParameterDirection.Input
                                );
                            }

                            cmdSel.CommandText =
                                @"
                        SELECT DOCUMENTO,
                               MAX(FECHAEMISION) AS FECHAEMISION
                        FROM VENDOCENCFED
                        WHERE FECHAEMISION IS NOT NULL
                          AND  TIPODOC IN ('01','03','04','09')
                          AND CIA = :pCIA
                          AND
                          (
                              DOCUMENTO IN (" +
                                      string.Join(",", parametrosNormales) +
                                      @")
                              OR
                              DOCUMENTO IN (" +
                                      string.Join(",", parametros08) +
                                      @")
                          )
                        GROUP BY DOCUMENTO";

                            // ================================================
                            // Diccionario:
                            //
                            // NUM_FACTURA_ORACLE -> FECHAEMISION
                            // ================================================
                            var fechasEncontradas =
                                new Dictionary<string, DateTime>(
                                    StringComparer.OrdinalIgnoreCase
                                );

                            using (var rd = cmdSel.ExecuteReader())
                            {
                                while (rd.Read())
                                {
                                    if (rd["DOCUMENTO"] == DBNull.Value ||
                                        rd["FECHAEMISION"] == DBNull.Value)
                                    {
                                        continue;
                                    }

                                    string documento =
                                        rd["DOCUMENTO"]
                                            .ToString()
                                            .Trim();

                                    // Quitar prefijo 08- si existe
                                    if (
                                        documento.StartsWith(
                                            "08-",
                                            StringComparison.OrdinalIgnoreCase
                                        )
                                    )
                                    {
                                        documento =
                                            documento.Substring(3);
                                    }

                                    DateTime fecha =
                                        Convert.ToDateTime(
                                            rd["FECHAEMISION"]
                                        );

                                    fechasEncontradas[documento] =
                                        fecha;
                                }
                            }

                            totalEncontradas11g +=
                                fechasEncontradas.Count;

                            // ================================================
                            // Si este bloque no encontró nada en 11g
                            // ================================================
                            if (fechasEncontradas.Count == 0)
                            {
                                totalNoEncontradas += bloque.Count;

                                _log(
                                    $"Bloque {numeroBloque}: " +
                                    $"ningún documento encontrado en 11g."
                                );

                                continue;
                            }

                            // ================================================
                            // Preparar arrays para UPDATE masivo ATP
                            // ================================================
                            var numerosActualizar =
                                new List<string>();

                            var fechasActualizar =
                                new List<DateTime>();

                            foreach (string numero in bloque)
                            {
                                DateTime fecha;

                                if (
                                    fechasEncontradas.TryGetValue(
                                        numero,
                                        out fecha
                                    )
                                )
                                {
                                    numerosActualizar.Add(numero);
                                    fechasActualizar.Add(fecha);
                                }
                                else
                                {
                                    totalNoEncontradas++;
                                }
                            }

                            if (numerosActualizar.Count == 0)
                            {
                                continue;
                            }

                            // ================================================
                            // 3. UPDATE MASIVO ATP CON ARRAY BIND
                            // ================================================
                            using (
                                var cmdUpd =
                                    conSrc.CreateCommand()
                            )
                            {
                                cmdUpd.BindByName = true;

                                cmdUpd.CommandText = @"
                            UPDATE VENDOCENCFED
                               SET FECHA_FACTURAELECTRONICA =
                                   :pFECHA
                             WHERE NUM_FACTURA_ORACLE =
                                   :pNUMERO
                               AND FECHA_FACTURAELECTRONICA
                                   IS NULL
                               AND  TIPODOC IN ('01','03','04','09')
                               AND CIA = :pCIA";

                                cmdUpd.ArrayBindCount =
                                    numerosActualizar.Count;

                                cmdUpd.Parameters.Add(
                                    ":pFECHA",
                                    OracleDbType.Date,
                                    fechasActualizar.ToArray(),
                                    ParameterDirection.Input
                                );

                                cmdUpd.Parameters.Add(
                                    ":pNUMERO",
                                    OracleDbType.Varchar2,
                                    numerosActualizar.ToArray(),
                                    ParameterDirection.Input
                                );

                                cmdUpd.Parameters.Add(
                                    ":pCIA",
                                    OracleDbType.Varchar2,
                                    Enumerable.Repeat(_ciaFiltro, numerosActualizar.Count).ToArray(),
                                    ParameterDirection.Input
                                );

                                int afectados =
                                    cmdUpd.ExecuteNonQuery();

                                totalActualizadas += afectados;

                                _log(
                                    $"Bloque {numeroBloque} actualizado | " +
                                    $"Encontradas11g={fechasEncontradas.Count} | " +
                                    $"EnviadasATP={numerosActualizar.Count} | " +
                                    $"ActualizadasATP={afectados}"
                                );
                            }
                        }
                    }
                }

                // ============================================================
                // RESUMEN FINAL
                // ============================================================
                _log("===========================================");

                _log(
                    $"FIN FECHA_FACTURAELECTRONICA | " +
                    $"PendientesATP={facturasPendientes.Count} | " +
                    $"Encontradas11g={totalEncontradas11g} | " +
                    $"NoEncontradas11g={totalNoEncontradas} | " +
                    $"ActualizadasATP={totalActualizadas} | " +
                    $"Bloques={numeroBloque}"
                );

                _log("===========================================");

                return totalActualizadas;
            }
            catch (Exception ex)
            {
                _log("===========================================");
                _log(
                    "ERROR SINCRONIZANDO FECHA_FACTURAELECTRONICA"
                );

                _log(
                    $"Mensaje    : {ex.Message}"
                );

                if (ex.InnerException != null)
                {
                    _log(
                        $"Inner      : {ex.InnerException.Message}"
                    );
                }

                _log(
                    $"StackTrace : {ex.StackTrace}"
                );

                _log("===========================================");

                throw;
            }
        }

    }
}