using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class Program
    {
        public static void ReadConfigInfo(ConfigInfo dbInfo, string line)
        {
            if (line.Trim().StartsWith("dataSource"))
            {
                string[] parts = line.Split('=');
                dbInfo.DataSource = parts[1].Trim();
            }

            if (line.Trim().StartsWith("port"))
            {
                string[] parts = line.Split('=');
                int port;
                bool parsed = Int32.TryParse(parts[1].Trim(), out port);
                if (parsed)
                {
                    dbInfo.Port = port;
                }
                else
                {
                    throw new WrongPortFormatException();
                }

            }

            if (line.Trim().StartsWith("userName"))
            {
                string[] parts = line.Split('=');
                dbInfo.UserName = parts[1].Trim();
            }

            if (line.Trim().StartsWith("password"))
            {
                string[] parts = line.Split('=');
                dbInfo.Password = parts[1].Trim();
            }

            if (line.Trim().StartsWith("databaseName"))
            {
                string[] parts = line.Split('=');
                dbInfo.DatabaseName = parts[1].Trim();
            }            
        }
        public static LocalDBConfigInfo ReadLocalDBConfigFile(string fileName)
        {
            LocalDBConfigInfo dbInfo = new LocalDBConfigInfo();

            const Int32 BufferSize = 256;
            using (var fileStream = File.OpenRead(fileName))
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, BufferSize))
            {
                String line;
                while ((line = streamReader.ReadLine()) != null)
                {                   
                    ReadConfigInfo(dbInfo, line);

                    if (line.Trim().StartsWith("tableName"))
                    {
                        string[] parts = line.Split('=');
                        dbInfo.TableName = parts[1].Trim();
                    }
                }      
            }
            return dbInfo;
        }

        public static MediawikiDBConfigInfo ReadMediawikiConfigFile(string fileName)
        {
            MediawikiDBConfigInfo mwInfo = new MediawikiDBConfigInfo();

            const Int32 BufferSize = 256;
            using (var fileStream = File.OpenRead(fileName))
            using (var streamReader = new StreamReader(fileStream, Encoding.UTF8, true, BufferSize))
            {
                String line;
                while ((line = streamReader.ReadLine()) != null)
                {
                    ReadConfigInfo(mwInfo, line);

                    if (line.Trim().StartsWith("tableName_page"))
                    {
                        string[] parts = line.Split('=');
                        mwInfo.MwPageTable = parts[1].Trim();
                    }

                    if (line.Trim().StartsWith("tableName_text"))
                    {
                        string[] parts = line.Split('=');
                        mwInfo.MwTextTable = parts[1].Trim();
                    }
                }
            }
            return mwInfo;
        }

        static List<MwPageData> ReadMwPageData(string connectionString, string query)
        {
            List<MwPageData> mwPageData = new List<MwPageData>();

            // Prepare the connection
            MySqlConnection databaseConnection = new MySqlConnection(connectionString);
            MySqlCommand commandDatabase = new MySqlCommand(query, databaseConnection);
            commandDatabase.CommandTimeout = 60;
            MySqlDataReader reader;

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
                        byte[] title = (byte[])reader["page_title"];
                        int latest = Convert.ToInt32(reader["page_latest"]);
                        mwPageData.Add(new MwPageData(id, title, latest));
                    }
                }
                else
                {
                    Console.WriteLine("No rows found.");
                    return null;
                }

                foreach (MwPageData page in mwPageData)
                {
                    Console.WriteLine("=======================================================================");
                    Console.WriteLine(String.Format(" Id: {0}\n Title: {1}\n Latest: {2}", page.PageId, page.GetStringTitleName(), page.PageLatest));
                    Console.WriteLine("=======================================================================");        
                }
                // Finally close the connection
                databaseConnection.Close();
                return mwPageData;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw new MwPageTableConnectionException(ex.Message);
            }                       
        }

        static List<MwTextData> ReadMwtextData(string connectionString, string query)
        {
            List<MwTextData> mwTextData = new List<MwTextData>();

            // Prepare the connection
            MySqlConnection databaseConnection = new MySqlConnection(connectionString);
            MySqlCommand commandDatabase = new MySqlCommand(query, databaseConnection);
            commandDatabase.CommandTimeout = 60;
            MySqlDataReader reader;

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
                        int id = Convert.ToInt32(reader["old_id"]);
                        byte[] content = (byte[])reader["old_text"];
                        
                        mwTextData.Add(new MwTextData(id, content));
                    }
                }
                else
                {
                    Console.WriteLine("No rows found.");
                    return null;
                }

                foreach (MwTextData page in mwTextData)
                {
                    Console.WriteLine("=======================================================================");
                    Console.WriteLine(String.Format(" Id: {0}\n Content: {1}", page.Id, ASCIIEncoding.ASCII.GetString(page.Text)));
                    Console.WriteLine("=======================================================================");
                }
                // Finally close the connection
                databaseConnection.Close();
                return mwTextData;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw new MwTextTableConnectionException(ex.Message);
            }
        }

        public static List<LocalDBPage> ReadMediawikiPages()
        {
            MediawikiDBConfigInfo mwInfo = ReadMediawikiConfigFile("mediawiki.txt");

            string connectionString = mwInfo.GetConnectionString();

            //reading table mw_pages
            string queryMwPages = String.Format("SELECT * FROM {0}",mwInfo.MwPageTable);

            List<MwPageData> mwPageData = ReadMwPageData(connectionString, queryMwPages);

            string queryMwText = String.Format("SELECT * FROM {0}", mwInfo.MwTextTable);

            List<MwTextData> mwTextData = ReadMwtextData(connectionString, queryMwText);

            return null;
        }

        public static List<LocalDBPage> ReadLocalPages()
        {
            LocalDBConfigInfo dbInfo = ReadLocalDBConfigFile("local_pages.txt");
            
            string connectionString = dbInfo.GetConnectionString();
            // Your query,            
            string query = String.Format("SELECT * FROM {0}", dbInfo.TableName);

            // Prepare the connection
            MySqlConnection databaseConnection = new MySqlConnection(connectionString);
            MySqlCommand commandDatabase = new MySqlCommand(query, databaseConnection);
            commandDatabase.CommandTimeout = 60;
            MySqlDataReader reader;

            List<LocalDBPage> localDb = new List<LocalDBPage>();
            
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

            List<LocalDBPage> mwPageDatas = ReadMediawikiPages();
            if (mwPageDatas == null)
            {
                Console.WriteLine("Mw_page database is null, nothing to update ...");
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
                Console.WriteLine(e.Message);
            }
            catch (WrongPortFormatException e)
            {
                Console.WriteLine("There is wrong format of port number in the config file for local database.");
                Console.WriteLine(e.Message);
            }
            catch (MwPageTableConnectionException e)
            {
                Console.WriteLine("There were some troubles while reading the mw_page table.");
                Console.WriteLine("Unable to update pages.");
                Console.WriteLine(e.Message);
            }
            catch (MwTextTableConnectionException e)
            {
                Console.WriteLine("There were some troubles while reading the mw_text table.");
                Console.WriteLine("Unable to update pages.");
                Console.WriteLine(e.Message);
            }
            
        }
    }
}
