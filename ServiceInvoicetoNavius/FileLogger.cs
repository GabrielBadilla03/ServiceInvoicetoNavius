using System;
using System.IO;
using System.Text;

namespace ServiceInvoicetoNavius
{
    public class FileLogger
    {
        private readonly string _dir;
        private readonly object _lock = new object();

        public FileLogger(string dir)
        {
            _dir = string.IsNullOrWhiteSpace(dir) ? @"C:\ServicioLog" : dir;
            Directory.CreateDirectory(_dir);
        }

        private string LogFilePath()
        {
            string name = $"ServiceInvoicetoNavius_{DateTime.Now:yyyyMMdd}.log";
            return Path.Combine(_dir, name);
        }

        public void Info(string message) => Write("INFO", message);

        public void Error(string message, Exception ex)
        {
            Write("ERROR", $"{message} | {ex.Message}");
            Write("ERROR", ex.StackTrace ?? "");

            if (ex.InnerException != null)
            {
                Write("ERROR", $"INNER | {ex.InnerException.Message}");
                Write("ERROR", ex.InnerException.StackTrace ?? "");
            }
        }

        private void Write(string level, string message)
        {
            lock (_lock)
            {
                var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level}] {message}";
                File.AppendAllText(
                    LogFilePath(),
                    line + Environment.NewLine,
                    Encoding.UTF8
                );
            }
        }
    }
}