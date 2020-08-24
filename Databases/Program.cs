using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class Program
    {
        public LocalDBConfigInfo ReadLocalDBConfigFile(string path)
        {
            return null;
        }

        public static List<LocalDBPage> ReadLocalPages()
        {
            LocalDBConfigInfo dbInfo = new LocalDBConfigInfo();
            dbInfo.DataSource = "127.0.0.1";
            dbInfo.Port = 3306;
            dbInfo.UserName = "root";
            dbInfo.Password = "password";
            dbInfo.DatabaseName = "local_page_database";
            dbInfo.TableName = "local_pages";

            string connectionString = dbInfo.GetConnectionString();
            // Your query,            
            string query = String.Format("SELECT * FROM {0}", dbInfo.TableName);

            // Prepare the connection
            MySqlConnection databaseConnection = new MySqlConnection(connectionString);
            MySqlCommand commandDatabase = new MySqlCommand(query, databaseConnection);
            commandDatabase.CommandTimeout = 60;
            MySqlDataReader reader;

            List<LocalDBPage> localDb = new List<LocalDBPage>();
            // Let's do it !
            try
            {
                // Open the database
                databaseConnection.Open();

                // Execute the query
                reader = commandDatabase.ExecuteReader();
                                
                if (reader.HasRows)
                {
                    while (reader.Read())
                    {                        
                        int id = Convert.ToInt32(reader["page_id"]);
                        string title = reader["page_title"].ToString();
                        string hash = reader["page_hash"].ToString();
                        byte[] content = (byte[])reader["page_content"];
                        localDb.Add(new LocalDBPage(id, title, hash, content));
                    }
                }
                else
                {
                    Console.WriteLine("No rows found.");
                    return null;
                }

                foreach (LocalDBPage page in localDb)
                {
                    Console.WriteLine(String.Format(" Id: {0}\n Title: {1}\n Hash: {2}", page.PageId, page.PageTitle, page.PageHash));
                    Console.WriteLine("=======================================================================");
                    Console.WriteLine(ASCIIEncoding.ASCII.GetChars(page.PageContent));
                    Console.WriteLine("=======================================================================");

                }
                // Finally close the connection
                databaseConnection.Close();
                return localDb;
            }
            catch (Exception ex)
            {
                // Show any error message.
                Console.WriteLine(ex.Message);
                throw new LocalDatabaseConnectionException(ex.Message);
                
            }
        }

        public static void Update(string[] args)
        {
            List<LocalDBPage> localDBPages = ReadLocalPages();
            if (localDBPages == null)
            {
                Console.WriteLine("Local database for pages is null, nothing to update ...");
                Console.WriteLine("Ending program ...");
                return;
            }
        }
        static void Main(string[] args)
        {
            try
            {
                Update(args);
            }
            catch (LocalDatabaseConnectionException e)
            {
                Console.WriteLine("There were some troubles while trying to connect to local database.");
                Console.WriteLine("The exception message text: ");
                Console.WriteLine(e.Message);
            }
            
        }
    }
}
