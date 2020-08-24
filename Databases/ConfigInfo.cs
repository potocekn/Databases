using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    public abstract class ConfigInfo
    {
        public string DataSource { get; set; }
        public int Port { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string DatabaseName { get; set; }

        public string GetConnectionString()
        {
            return String.Format("datasource={0};port={1};username={2};password={3};database={4};", DataSource, Port, UserName, Password, DatabaseName);
        }
    }
}
