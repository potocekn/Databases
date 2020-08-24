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
        public string[] ReadLocalDBConfigFile(string path)
        {

            return null;
        }

        public static void Update(string[] args)
        {
            string dataSource = "127.0.0.1";
            string port = "3306";
            string userName = "root";
            string passwd = "password";
            string databaseName = "local_page_database";
            string connectionString = String.Format("datasource={0};port={1};username={2};password={3};database={4};",dataSource,port,userName,passwd, databaseName);
            // Your query,
            string tableName = "local_pages";
            string query = String.Format("SELECT * FROM {0}", tableName);

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

                // All succesfully executed, now do something

                // IMPORTANT : 
                // If your query returns result, use the following processor :

                if (reader.HasRows)
                {
                    while (reader.Read())
                    {
                        // As our database, the array will contain : ID 0, FIRST_NAME 1,LAST_NAME 2, ADDRESS 3
                        // Do something with every received database ROW
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
                }

                foreach (LocalDBPage page in localDb)
                {
                    Console.WriteLine(String.Format(" Id: {0}\n Title: {1}\n Hash: {2}",page.PageId, page.PageTitle, page.PageHash));
                    Console.WriteLine("=======================================================================");
                    Console.WriteLine(ASCIIEncoding.ASCII.GetChars(page.PageContent));
                    Console.WriteLine("=======================================================================");

                }
                // Finally close the connection
                databaseConnection.Close();
            }
            catch (Exception ex)
            {
                // Show any error message.
                Console.WriteLine(ex.Message);
            }
        }
        static void Main(string[] args)
        {
            Update(args);
        }
    }
}
