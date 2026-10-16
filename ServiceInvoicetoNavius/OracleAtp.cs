using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Globalization;
using System.Linq;
using System.Text;
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
            Action<string> logger = null)
        {
            _log = logger ?? (_ => { });
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
                           AND CIA = :pCIA";

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

        public int VENDOCENCFED()
        {
            ValidarIndicesDestino();

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
                ORDER BY COD_RUTA ASC";

            var dt = new DataTable();

            using (var conSrc = OpenSrc())
            using (var cmd = conSrc.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.InitialLOBFetchSize = -1;
                cmd.CommandText = selectBase;
                cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                using (var da = new OracleDataAdapter(cmd))
                    da.Fill(dt);

                if (dt.Rows.Count == 0)
                {
                    _log("No hay registros pendientes INDTRASLADO='N'.");
                    return 0;
                }

                _log($"ATP cabeceras leídas: {dt.Rows.Count}");
            }

            var facturas = dt.Rows
                .Cast<DataRow>()
                .Where(r => !string.IsNullOrWhiteSpace(r["NUM_FACTURA_ORACLE"]?.ToString()))
                .Select(r => new FacturaInfo
                {
                    NumFacturaOracle = r["NUM_FACTURA_ORACLE"]?.ToString()?.Trim(),
                    Documento = r["DOCUMENTO"]?.ToString()?.Trim(),
                    Clave = r["CLAVE"]?.ToString()?.Trim()
                })
                .GroupBy(x => x.NumFacturaOracle)
                .Select(g => g.First())
                .ToList();

            var facturasValidas = new List<FacturaInfo>();
            var rowsCabeceraValidas = dt.Clone();

            foreach (var factura in facturas)
            {
                string errorCabecera;

                if (ExisteCabeceraEnDestino(factura, out errorCabecera))
                {
                    MarcarFacturaConError(factura, errorCabecera);
                    _log($"Cabecera omitida por duplicado | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle}");
                    continue;
                }

                var rows = dt.Rows
                    .Cast<DataRow>()
                    .Where(r => r["NUM_FACTURA_ORACLE"]?.ToString()?.Trim() == factura.NumFacturaOracle);

                foreach (var row in rows)
                    rowsCabeceraValidas.ImportRow(row);

                facturasValidas.Add(factura);
            }

            if (facturasValidas.Count == 0)
            {
                _log("No hay facturas válidas para trasladar. Todas quedaron con error.");
                return dt.Rows.Count;
            }

            BulkCopyConDiagnostico(
                rowsCabeceraValidas,
                "XXORA_VENDOCENCFED",
                "Traslado de cabeceras VENDOCENCFED");

            _log($"Bulk cabeceras OK en 11g. Insertadas={rowsCabeceraValidas.Rows.Count}");

            var resultados = CopiarDetallesEnParalelo(facturasValidas);

            var exitosas = resultados
                .Where(x => x.Ok)
                .Select(x => x.Factura.NumFacturaOracle)
                .Distinct()
                .ToList();

            var fallidas = resultados
                .Where(x => !x.Ok)
                .ToList();

            foreach (var f in fallidas)
            {
                _log("===========================================");
                _log("FACTURA NO MARCADA COMO TRASLADADA");
                _log($"Documento      : {f.Factura.Documento}");
                _log($"Factura Oracle : {f.Factura.NumFacturaOracle}");
                _log($"Clave FE       : {f.Factura.Clave}");
                _log($"Error          : {f.Error}");
                _log("===========================================");
            }

            if (exitosas.Count > 0)
            {
                using (var conSrc = OpenSrc())
                using (var upd = conSrc.CreateCommand())
                {
                    upd.BindByName = true;
                    upd.CommandText = @"
                        UPDATE VENDOCENCFED
                           SET INDTRASLADO = 'S'
                         WHERE NUM_FACTURA_ORACLE = :p
                           AND CIA = :pCIA";

                    upd.ArrayBindCount = exitosas.Count;

                    var ciasExitosas = Enumerable.Repeat(_ciaFiltro, exitosas.Count).ToArray();

                    upd.Parameters.Add(":p", OracleDbType.Varchar2, exitosas.ToArray(), ParameterDirection.Input);
                    upd.Parameters.Add(":pCIA", OracleDbType.Varchar2, ciasExitosas, ParameterDirection.Input);

                    int afectados = upd.ExecuteNonQuery();
                    _log($"ATP marcados INDTRASLADO='S': {afectados}");
                }
            }

            _log($"Resumen traslado: Exitosas={exitosas.Count}, Fallidas={fallidas.Count}");

            return dt.Rows.Count;
        }

        private List<ResultadoDetalle> CopiarDetallesEnParalelo(IList<FacturaInfo> facturas)
        {
            var resultados = new List<ResultadoDetalle>();
            var lockObj = new object();

            var po = new ParallelOptions { MaxDegreeOfParallelism = _maxDegree };

            Parallel.ForEach(facturas, po, factura =>
            {
                string etapa = "";

                try
                {
                    etapa = "Leyendo VENDOCDETFED desde ATP";

                    var dtDet = new DataTable();

                    using (var conSrc = OpenSrc())
                    using (var cmd = conSrc.CreateCommand())
                    {
                        cmd.BindByName = true;
                        cmd.CommandText = @"
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
                                   ID_FACTURA_ORACLE, NUM_FACTURA_ORACLE, BULTOS, COMENTARIOS, LOTE,source_schedule_number,CODIGO_IMPUESTO,TASA_IMPUESTO
                            FROM VENDOCDETFED
                            WHERE NUM_FACTURA_ORACLE = :p
                              AND CIA = :pCIA";

                        cmd.Parameters.Add(":p", OracleDbType.Varchar2, factura.NumFacturaOracle, ParameterDirection.Input);
                        cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                        using (var da = new OracleDataAdapter(cmd))
                            da.Fill(dtDet);
                    }

                    etapa = "Validando PK detalle en destino";

                    string errorPkDetalle;
                    if (ExisteDetalleEnDestino(dtDet, factura, out errorPkDetalle))
                    {
                        MarcarFacturaConError(factura, errorPkDetalle);

                        lock (lockObj)
                        {
                            resultados.Add(new ResultadoDetalle
                            {
                                Factura = factura,
                                Ok = false,
                                Error = errorPkDetalle
                            });
                        }

                        return;
                    }

                    etapa = "Leyendo VENDOCDETIMPFED desde ATP";

                    var dtImp = new DataTable();

                    using (var conSrc = OpenSrc())
                    using (var cmd = conSrc.CreateCommand())
                    {
                        cmd.BindByName = true;
                        cmd.CommandText = @"
                            SELECT CIA, CLAVE, LINEADETALLE,
                                   CODIGO, TARIFA, MONTO,
                                   EXONERA_TIPODOC, EXONERA_NUMDOC, EXONERA_NOMINSTIT,
                                   EXONERA_FECHAEMISION, EXONERA_MONTOIMP, EXONERA_PORCCOMPRA,
                                   CODIGOTARIFA, FACTORIVA, CODIGO_COMBO,
                                   CODIGO_BONIF, EXONERA_ARTICULO, EXONERA_INCISO, NUM_FACTURA_ORACLE
                            FROM VENDOCDETIMPFED
                            WHERE NUM_FACTURA_ORACLE = :p
                              AND CIA = :pCIA";

                        cmd.Parameters.Add(":p", OracleDbType.Varchar2, factura.NumFacturaOracle, ParameterDirection.Input);
                        cmd.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

                        using (var da = new OracleDataAdapter(cmd))
                            da.Fill(dtImp);
                    }

                    etapa = "Validando PK impuesto en destino";

                    string errorPkImp;
                    if (ExisteImpuestoEnDestino(dtImp, factura, out errorPkImp))
                    {
                        MarcarFacturaConError(factura, errorPkImp);

                        lock (lockObj)
                        {
                            resultados.Add(new ResultadoDetalle
                            {
                                Factura = factura,
                                Ok = false,
                                Error = errorPkImp
                            });
                        }

                        return;
                    }

                    etapa = "Insertando XXORA_VENDOCDETFED en 11g";

                    if (dtDet.Rows.Count > 0)
                    {
                        BulkCopyConDiagnostico(
                            dtDet,
                            "XXORA_VENDOCDETFED",
                            $"Detalle | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle}");
                    }

                    etapa = "Insertando XXORA_VENDOCDETIMPFED en 11g";

                    if (dtImp.Rows.Count > 0)
                    {
                        BulkCopyConDiagnostico(
                            dtImp,
                            "XXORA_VENDOCDETIMPFED",
                            $"Impuesto | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle}");
                    }

                    _log($"Detalle OK | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle} | LineasDetalle={dtDet.Rows.Count} | LineasImpuesto={dtImp.Rows.Count}");

                    lock (lockObj)
                    {
                        resultados.Add(new ResultadoDetalle
                        {
                            Factura = factura,
                            Ok = true,
                            Error = null
                        });
                    }
                }
                catch (Exception ex)
                {
                    string errorCompleto =
                        $"Etapa={etapa} | Documento={factura.Documento} | FacturaOracle={factura.NumFacturaOracle} | Clave={factura.Clave} | Error={ex.Message}";

                    MarcarFacturaConError(factura, errorCompleto);

                    _log("===========================================");
                    _log("ERROR COPIANDO DETALLE");
                    _log($"Etapa          : {etapa}");
                    _log($"Documento      : {factura.Documento}");
                    _log($"Factura Oracle : {factura.NumFacturaOracle}");
                    _log($"Clave FE       : {factura.Clave}");
                    _log($"Mensaje        : {ex.Message}");

                    if (ex.InnerException != null)
                        _log($"InnerException : {ex.InnerException.Message}");

                    _log($"StackTrace     : {ex.StackTrace}");
                    _log("===========================================");

                    lock (lockObj)
                    {
                        resultados.Add(new ResultadoDetalle
                        {
                            Factura = factura,
                            Ok = false,
                            Error = errorCompleto
                        });
                    }
                }
            });

            return resultados;
        }

        public void EjecutarGuardarFacturas(int pLote)
        {
            using (var conDst = OpenDst())
            using (var cmd = conDst.CreateCommand())
            {
                cmd.BindByName = true;
                cmd.CommandType = CommandType.StoredProcedure;
                cmd.CommandText = "NUEVO.facturaElecOracle.GuardarFacturas";
                cmd.CommandTimeout = 0;

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

                try
                {
                    using (var conSrc = OpenSrc())
                    {
                        string rowIdDocumento = null;
                        string claveActual = null;

                        // ====================================================
                        // 3. Buscar la fila exacta del documento en ATP
                        // ====================================================
                        using (var cmdBuscar = conSrc.CreateCommand())
                        {
                            cmdBuscar.BindByName = true;
                            cmdBuscar.CommandText = @"
                                SELECT ROWIDTOCHAR(ROWID) AS RID,
                                       CLAVE
                                  FROM VENDOCENCFED
                                 WHERE NUM_FACTURA_ORACLE = :pDOCUMENTO
                                   AND CIA = :pCIA
                                   AND ROWNUM = 1";

                            cmdBuscar.Parameters.Add(
                                ":pDOCUMENTO",
                                OracleDbType.Varchar2,
                                documento,
                                ParameterDirection.Input);

                            cmdBuscar.Parameters.Add(
                                ":pCIA",
                                OracleDbType.Varchar2,
                                _ciaFiltro,
                                ParameterDirection.Input);

                            using (var rd = cmdBuscar.ExecuteReader())
                            {
                                if (rd.Read())
                                {
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
                                   AND CIA = :pCIA";

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

                    cmdSel.CommandText = @"
                SELECT CLAVE,
                       NUMEROCONSECUTIVO,
                       ESTADO,
                       MENSAJE_HACIENDA
                FROM VENDOCENCFED
                WHERE DOCUMENTO = :pNUM_FACTURA_ORACLE
                  AND ESTADO = 'A'
                  AND CIA = :pCIA
                  AND ROWNUM = 1";

                    var pNumero11g = cmdSel.Parameters.Add(
                        ":pNUM_FACTURA_ORACLE",
                        OracleDbType.Varchar2
                    );

                    cmdSel.Parameters.Add(":pCIA", OracleDbType.Varchar2, _ciaFiltro, ParameterDirection.Input);

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
                   AND CIA = :pCIA";

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

                    // ========================================================
                    // Procesar facturas
                    // ========================================================
                    foreach (string numeroFactura in facturasPendientes)
                    {
                        pNumero11g.Value = numeroFactura;

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

                            int afectados = cmdUpdSrc.ExecuteNonQuery();

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