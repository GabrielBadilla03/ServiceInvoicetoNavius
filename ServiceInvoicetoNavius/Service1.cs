using System;
using System.Configuration;
using System.Globalization;
using System.ServiceProcess;
using System.Threading;
using System.Threading.Tasks;

namespace ServiceInvoicetoNavius
{
    public partial class Service1 : ServiceBase
    {
        private System.Timers.Timer _timer;
        private int _running = 0;
        private FileLogger _log;

        // Mantener una sola instancia de OracleAtp durante la vida del servicio.
        private OracleAtp _proceso;

        // ARREGLO 8: las conexiones se verifican en el primer ciclo, no en OnStart.
        private bool _conexionesVerificadas = false;

        // ARREGLO 8: se activa en OnStop para que el ciclo en curso no siga con pasos nuevos.
        private volatile bool _deteniendo = false;

        // REVISION v5: OnStop debe esperar al menos lo que puede durar GuardarFacturas;
        // si mata el proceso a mitad del lote, el paquete revierte todo el lote.
        private int _timeoutGuardarSeg = 600;

        public Service1()
        {
            InitializeComponent();
            this.ServiceName = "ServiceInvoicetoNavius";
        }

        protected override void OnStart(string[] args)
        {
            // =============================================================
            // ARREGLO 8: OnStart solo lee la configuración y arranca el timer.
            // Windows espera unos 30 segundos a que OnStart termine; si no
            // termina, el servicio se queda en "Iniciando" y no se puede
            // detener. Por eso aquí NO se conecta a la base ni se procesa
            // ninguna factura: eso se hace en segundo plano (RunJobSafe).
            // =============================================================
            try
            {
                var logDir = ConfigurationManager.AppSettings["LogDir"] ?? @"C:\ServicioLog";
                _log = new FileLogger(logDir);

                _log.Info("===============================================");
                _log.Info("Iniciando servicio ServiceInvoicetoNavius...");
                _log.Info("===============================================");

                // =========================================================
                // Leer configuración
                // =========================================================
                string walletPath = ConfigurationManager.AppSettings["WalletPath"];
                string tnsAliasOrigen = ConfigurationManager.AppSettings["TnsAliasOrigen"];
                string walletPassword = ConfigurationManager.AppSettings["WalletPassword"];
                string userOrigen = ConfigurationManager.AppSettings["UserOrigen"];
                string passOrigen = ConfigurationManager.AppSettings["PassOrigen"];

                string dataSource11gTns = ConfigurationManager.AppSettings["DataSource11gTns"];
                string userDestino = ConfigurationManager.AppSettings["UserDestino"];
                string passDestino = ConfigurationManager.AppSettings["PassDestino"];

                // =========================================================
                // CIA que procesará este servicio
                // Ejemplo: LANCO_RD
                // =========================================================
                string ciaFiltro = ConfigurationManager.AppSettings["CiaFiltro"];

                if (string.IsNullOrWhiteSpace(ciaFiltro))
                {
                    throw new ConfigurationErrorsException(
                        "No se encontró la configuración CiaFiltro en App.config."
                    );
                }

                ciaFiltro = ciaFiltro.Trim();

                int maxDegree = Math.Max(1, Math.Min(4, LeerEntero("MaxDegree", 4)));
                int intervalMs = Math.Max(10000, LeerEntero("IntervalMs", 60000));

                // ARREGLO 8: opciones del traslado. Todas son opcionales en el .config.
                var opciones = new OracleAtp.OpcionesTraslado
                {
                    MaxFacturasPorCiclo = Math.Max(1, Math.Min(100, LeerEntero("MaxFacturasPorCiclo", 10))),
                    ValidarMontos = LeerBooleano("ValidarMontos", true),
                    ToleranciaMonto = LeerDecimal("ToleranciaMonto", 1.00m),
                    MaxCiclosEspera = LeerEntero("MaxCiclosEspera", 5),
                    // REVISION v5
                    CiclosReintentoRevision = Math.Max(1, LeerEntero("CiclosReintentoRevision", 15)),
                    TimeoutGuardarFacturasSeg = Math.Max(0, LeerEntero("TimeoutGuardarFacturasSeg", 600))
                };

                _timeoutGuardarSeg = opciones.TimeoutGuardarFacturasSeg;

                _log.Info($"CIA configurada para el servicio: {ciaFiltro}");
                _log.Info(
                    $"Opciones: MaxDegree={maxDegree} | IntervalMs={intervalMs} | " +
                    $"MaxFacturasPorCiclo={opciones.MaxFacturasPorCiclo} | ValidarMontos={opciones.ValidarMontos} | " +
                    $"ToleranciaMonto={opciones.ToleranciaMonto} | MaxCiclosEspera={opciones.MaxCiclosEspera} | " +
                    $"CiclosReintentoRevision={opciones.CiclosReintentoRevision} | " +
                    $"LoteGuardarFacturas={LeerLoteGuardarFacturas()} | TimeoutGuardarFacturasSeg={opciones.TimeoutGuardarFacturasSeg}");
                _log.Info("Inicializando OracleAtp...");

                // =========================================================
                // Crear OracleAtp una sola vez (no abre conexiones)
                // =========================================================
                _proceso = new OracleAtp(
                    walletPath: walletPath,
                    tnsAliasOrigen: tnsAliasOrigen,
                    walletPassword: walletPassword,
                    userOrigen: userOrigen,
                    passOrigen: passOrigen,
                    dataSource11gTns: dataSource11gTns,
                    userDestino: userDestino,
                    passDestino: passDestino,
                    ciaFiltro: ciaFiltro,
                    maxDegree: maxDegree,
                    logger: msg => _log.Info(msg),
                    opciones: opciones
                );

                // =========================================================
                // Configurar Timer
                // =========================================================
                _timer = new System.Timers.Timer(intervalMs);
                _timer.AutoReset = true;
                _timer.Elapsed += (s, e) => RunJobSafe();
                _timer.Start();

                _log.Info(
                    $"Servicio iniciado correctamente. Intervalo={intervalMs} ms | CIA={ciaFiltro}"
                );

                // ARREGLO 8: primera ejecución en segundo plano, para que OnStart
                // termine de inmediato y Windows marque el servicio "En ejecución".
                Task.Run(() => RunJobSafe());
            }
            catch (Exception ex)
            {
                if (_log != null)
                {
                    _log.Error("Error iniciando el servicio.", ex);
                }

                throw;
            }
        }

        protected override void OnStop()
        {
            try
            {
                // ARREGLO 6 y 8: no se empiezan pasos ni facturas nuevas...
                _deteniendo = true;
                _proceso?.SolicitarDetencion();
                _timer?.Stop();

                // ...y se espera a que termine lo que está en curso.
                // REVISION v5: mínimo 5 minutos, o el timeout de GuardarFacturas + 1 minuto.
                int esperaSeg = Math.Max(300, (_timeoutGuardarSeg > 0 ? _timeoutGuardarSeg : 1800) + 60);
                var limite = DateTime.Now.AddSeconds(esperaSeg);

                while (Interlocked.CompareExchange(ref _running, 0, 0) == 1 && DateTime.Now < limite)
                {
                    RequestAdditionalTime(15000);
                    Thread.Sleep(1000);
                }

                if (Interlocked.CompareExchange(ref _running, 0, 0) == 1)
                    _log?.Info($"Servicio detenido con un ciclo todavía en curso (se superaron {esperaSeg} segundos).");

                _timer?.Dispose();
                _log?.Info("Servicio detenido.");
            }
            catch (Exception ex)
            {
                _log?.Error("Error al detener el servicio", ex);
            }
        }

        private void RunJobSafe()
        {
            if (_deteniendo)
            {
                return;
            }

            // Evita que se ejecute un nuevo ciclo si el anterior todavía está activo.
            if (Interlocked.Exchange(ref _running, 1) == 1)
            {
                return;
            }

            try
            {
                RunJob();
            }
            catch (Exception ex)
            {
                _log.Error("Error general en RunJobSafe", ex);
            }
            finally
            {
                Interlocked.Exchange(ref _running, 0);
            }
        }

        private void RunJob()
        {
            int lote = LeerLoteGuardarFacturas();

            // =========================================================
            // 0. ARREGLO 8: verificar conexiones (antes estaba en OnStart).
            //    Si falla, se registra en el log y se reintenta en el
            //    siguiente ciclo, sin dejar el servicio en "Iniciando".
            // =========================================================
            if (!_conexionesVerificadas)
            {
                _log.Info("Verificando conexión a Oracle ATP y 11g...");
                _proceso.VerificarConexionesIniciales();
                _conexionesVerificadas = true;
                _log.Info("Conexiones verificadas correctamente.");
            }

            _log.Info("Iniciando ciclo...");

            // =========================================================
            // REVISION v5: cada paso va aislado. Si uno falla (por ejemplo ATP no
            // responde, o el paquete todavía no tiene el parámetro p_lote), se
            // registra y se sigue con los demás. Antes, un error en el paso 1
            // o 2 se saltaba todas las sincronizaciones del ciclo.
            // =========================================================

            // 1. Trasladar facturas ATP -> Oracle 11g (OracleAtp aplica el filtro de CIA)
            if (!Paso("VENDOCENCFED", () =>
                _log.Info($"VENDOCENCFED finalizó. Facturas trasladadas: {_proceso.VENDOCENCFED()}"))) return;

            // 2. Procesar facturas en Oracle 11g
            if (!Paso("GuardarFacturas", () =>
            {
                _log.Info($"Inicio de EjecutarGuardarFacturas (lote={lote})");
                _proceso.EjecutarGuardarFacturas(lote);
                _log.Info("GuardarFacturas ejecutado.");
            })) return;

            // 3. Sincronizar facturas aceptadas / procesadas
            if (!Paso("SincronizarEstadoHacienda_PorDocumento", () =>
                _log.Info($"SincronizarEstadoHacienda_PorDocumento procesó: {_proceso.SincronizarEstadoHacienda_PorDocumento()}"))) return;

            // 4. Facturas ATP sin clave que estén rechazadas en 11g
            if (!Paso("SincronizarFacturasRechazadasSinClave", () =>
                _log.Info($"SincronizarFacturasRechazadasSinClave actualizó: {_proceso.SincronizarFacturasRechazadasSinClave()}"))) return;

            // 5. Facturas ATP sin clave que estén aceptadas en 11g
            if (!Paso("SincronizarFacturasAceptadasSinClave", () =>
                _log.Info($"SincronizarFacturasAceptadasSinClave actualizó: {_proceso.SincronizarFacturasAceptadasSinClave()}"))) return;

            // 6. Sincronizar FECHA_FACTURAELECTRONICA 11g -> ATP
            Paso("SincronizarFechaFacturaElectronica", () =>
                _log.Info($"SincronizarFechaFacturaElectronica actualizó: {_proceso.SincronizarFechaFacturaElectronica()}"));

            _log.Info("Ciclo terminado.");
        }

        // REVISION v5: ejecuta un paso del ciclo; si falla lo registra y deja seguir.
        // Devuelve false solo si el servicio se está deteniendo (para cortar el ciclo).
        private bool Paso(string nombre, Action accion)
        {
            try
            {
                accion();
            }
            catch (Exception ex)
            {
                _log.Error($"ERROR en el paso {nombre}; se continúa con el siguiente paso", ex);
            }

            if (_deteniendo)
            {
                _log.Info("Ciclo interrumpido: el servicio se está deteniendo.");
                return false;
            }

            return true;
        }

        // REVISION v5: lote de GuardarFacturas (1..100). Por defecto 10.
        private int LeerLoteGuardarFacturas()
        {
            return Math.Max(1, Math.Min(100, LeerEntero("LoteGuardarFacturas", 10)));
        }

        // =================================================================
        // ARREGLO 8: lectura segura del .config. Si la llave no existe o
        // trae un valor inválido, se usa el valor por defecto y se avisa
        // en el log (antes un valor mal escrito tumbaba el arranque).
        // =================================================================
        private int LeerEntero(string llave, int porDefecto)
        {
            string valor = ConfigurationManager.AppSettings[llave];
            int resultado;

            if (string.IsNullOrWhiteSpace(valor))
                return porDefecto;

            if (int.TryParse(valor.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            _log?.Info($"Valor inválido en .config: {llave}='{valor}'. Se usa {porDefecto}.");
            return porDefecto;
        }

        private bool LeerBooleano(string llave, bool porDefecto)
        {
            string valor = ConfigurationManager.AppSettings[llave];
            bool resultado;

            if (string.IsNullOrWhiteSpace(valor))
                return porDefecto;

            if (bool.TryParse(valor.Trim(), out resultado))
                return resultado;

            _log?.Info($"Valor inválido en .config: {llave}='{valor}'. Se usa {porDefecto}.");
            return porDefecto;
        }

        private decimal LeerDecimal(string llave, decimal porDefecto)
        {
            string valor = ConfigurationManager.AppSettings[llave];
            decimal resultado;

            if (string.IsNullOrWhiteSpace(valor))
                return porDefecto;

            // Se acepta 1.00 o 1,00
            if (decimal.TryParse(valor.Trim().Replace(',', '.'), NumberStyles.Number, CultureInfo.InvariantCulture, out resultado))
                return resultado;

            _log?.Info($"Valor inválido en .config: {llave}='{valor}'. Se usa {porDefecto}.");
            return porDefecto;
        }
    }
}
