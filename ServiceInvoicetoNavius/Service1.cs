using System;
using System.Configuration;
using System.ServiceProcess;
using System.Threading;

namespace ServiceInvoicetoNavius
{
    public partial class Service1 : ServiceBase
    {
        private System.Timers.Timer _timer;
        private int _running = 0;
        private FileLogger _log;

        // Mantener una sola instancia de OracleAtp durante la vida del servicio.
        private OracleAtp _proceso;

        public Service1()
        {
            InitializeComponent();
            this.ServiceName = "ServiceInvoicetoNavius";
        }

        protected override void OnStart(string[] args)
        {
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

                int maxDegree = int.Parse(
                    ConfigurationManager.AppSettings["MaxDegree"] ?? "4"
                );

                _log.Info($"CIA configurada para el servicio: {ciaFiltro}");
                _log.Info("Inicializando OracleAtp...");

                // =========================================================
                // Crear OracleAtp una sola vez
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
                    logger: msg => _log.Info(msg)
                );

                // =========================================================
                // Verificar conexiones
                // =========================================================
                _log.Info("Verificando conexión a Oracle ATP...");
                _proceso.VerificarConexionesIniciales();
                _log.Info("Conexiones verificadas correctamente.");

                // =========================================================
                // Configurar Timer
                // =========================================================
                int intervalMs = int.Parse(
                    ConfigurationManager.AppSettings["IntervalMs"] ?? "60000"
                );

                _timer = new System.Timers.Timer(intervalMs);
                _timer.AutoReset = true;
                _timer.Elapsed += (s, e) => RunJobSafe();
                _timer.Start();

                _log.Info(
                    $"Servicio iniciado correctamente. Intervalo={intervalMs} ms | CIA={ciaFiltro}"
                );

                // Primera ejecución inmediata
                RunJobSafe();
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
                _timer?.Stop();
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
            int lote = int.Parse(
                ConfigurationManager.AppSettings["LoteGuardarFacturas"] ?? "1"
            );

            _log.Info("Iniciando ciclo...");

            // =========================================================
            // 1. Trasladar facturas ATP -> Oracle 11g
            //    OracleAtp aplica automáticamente el filtro de CIA.
            // =========================================================
            int copiadas = _proceso.VENDOCENCFED();

            _log.Info(
                $"VENDOCENCFED finalizó. Cabeceras copiadas: {copiadas}"
            );

            // =========================================================
            // 2. Procesar facturas en Oracle 11g
            // =========================================================
            _log.Info("Inicio de EjecutarGuardarFacturas");

            _proceso.EjecutarGuardarFacturas(lote);

            _log.Info("GuardarFacturas ejecutado. Ciclo OK.");

            // =========================================================
            // 3. Sincronizar facturas aceptadas / procesadas
            // =========================================================
            int sync = _proceso.SincronizarEstadoHacienda_PorDocumento();

            _log.Info(
                $"SincronizarEstadoHacienda_PorDocumento procesó: {sync}"
            );

            // =========================================================
            // 4. Buscar facturas ATP sin clave que estén rechazadas
            //    en Oracle 11g
            // =========================================================
            int rechazadas = _proceso.SincronizarFacturasRechazadasSinClave();

            _log.Info(
                $"SincronizarFacturasRechazadasSinClave actualizó: {rechazadas}"
            );

            // =========================================================
            // 5. Buscar facturas ATP sin clave que estén aceptadas
            //    en Oracle 11g
            // =========================================================
            int aceptadas = _proceso.SincronizarFacturasAceptadasSinClave();

            _log.Info(
                $"SincronizarFacturasAceptadasSinClave actualizó: {aceptadas}"
            );

            // =========================================================
            // 6. Sincronizar FECHA_FACTURAELECTRONICA
            //    Oracle 11g -> ATP
            // =========================================================
            int fechasActualizadas = _proceso.SincronizarFechaFacturaElectronica();

            _log.Info(
                $"SincronizarFechaFacturaElectronica actualizó: {fechasActualizadas}"
            );
        }
    }
}
