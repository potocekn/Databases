using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Security.Cryptography;
using System.Threading.Tasks;

namespace Databases
{
    class Program
    {
        /// <summary>
        /// This method reads configuration info that is common for all types of configuration files. 
        /// All common features are in abstract class ConfigInfo.
        /// This method is called when reading information for local database or for server mediawiki database. 
        /// Can throw WrongPortFormatException when the port number is in incorrect form (when it's not integer, ...)
        /// </summary>
        /// <param name="dbInfo">current config info that we extend based on what we read on the line.</param>
        /// <param name="line">represents line of document we want to process</param>
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

        /// <summary>
        /// This method reads configuration information from given file and puts it into LocalDBConfigInfo instance.
        /// This method sort of extends method ReadConfigInfo.
        /// </summary>
        /// <param name="fileName">represents the file that we would like to parse into LocalDBConfigInfo</param>
        /// <returns></returns>
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

        /// <summary>
        /// This method reads configuration information from given file and puts it into MediawikiDBConfigInfo instance.
        /// This method sort of extends method ReadConfigInfo.
        /// </summary>
        /// <param name="fileName">represents the file that we would like to parse into MediawikiDBConfigInfo</param>
        /// <returns></returns>
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

        /// <summary>
        /// This merhod connets to the mw_page table on mediawiki server and stores all important information from each row. 
        /// Data from each row are stored in MwPageData instances.
        /// </summary>
        /// <param name="connectionString">is needed in order to connect to the server</param>
        /// <param name="tableName">name of table that we want to read (mw_page)/param>
        /// <returns>The list of MwPageData instances for each row of table.</returns>
        static List<MwPageData> ReadMwPageData(string connectionString, string tableName)
        {
            List<MwPageData> mwPageData = new List<MwPageData>();
            string query = String.Format("SELECT * FROM {0}", tableName);

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

        /// <summary>
        /// This merhod connets to the mw_text table on mediawiki server and stores all important information from each row. 
        /// Data from each row are stored in MwTextData instances.
        /// </summary>
        /// <param name="connectionString">is needed in order to connect to the server</param>
        /// <param name="tableName">represents the table we want to read (mw_text)</param>
        /// <returns>The list of MwTextData instances for each row of table.</returns>
        static List<MwTextData> ReadMwTextData(string connectionString, string tableName)
        {
            List<MwTextData> mwTextData = new List<MwTextData>();
            string query = String.Format("SELECT * FROM {0}", tableName);

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

        /// <summary>
        /// This method reads data from mediawiki database.
        /// First method gets data from mw_page table, then gets data from mw_text. 
        /// Lastly both obtained information are combined into list of pages.
        /// </summary>
        /// <param name="mwInfo"></param>
        /// <returns>Method returns list of pages from mediawiki database.</returns>
        public static List<LocalDBPage> ReadMediawikiPages(MediawikiDBConfigInfo mwInfo)
        {           

            string connectionString = mwInfo.GetConnectionString();

            //reading table mw_pages
            List<MwPageData> mwPageData = ReadMwPageData(connectionString, mwInfo.MwPageTable);            

            //reading mw_text
            List<MwTextData> mwTextData = ReadMwTextData(connectionString, mwInfo.MwTextTable);

            MD5 md5 = MD5.Create();

            List<LocalDBPage> mediaWikiPages = (from x in mwPageData
                                                from y in mwTextData
                                                where x.PageLatest == y.Id
                                                select new LocalDBPage(x.PageId, x.GetStringTitleName(), HashToString(md5.ComputeHash(y.Text)), y.Text)
                                                ).ToList();

            return mediaWikiPages;
        }

        /// <summary>
        /// This method converts calculated hash (byte array) into string value.
        /// </summary>
        /// <param name="hashBytes">calculated hash</param>
        /// <returns>Method returns string value of given hash.</returns>
        static string HashToString(byte[] hashBytes)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < hashBytes.Length; i++)
            {
                sb.Append(hashBytes[i].ToString("X2"));
            }
            return sb.ToString();
        }

        /// <summary>
        /// This method reads information from local database and stores them as list of LocalDBPages.         
        /// </summary>
        /// <param name="dbInfo">Config info for local database.</param>
        /// <returns>List of read pages.</returns>
        public static List<LocalDBPage> ReadLocalPages(LocalDBConfigInfo dbInfo)
        {       
            
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

        /// <summary>
        /// This is the main method for updating mediawiki server.
        /// </summary>
        /// <param name="args"></param>
        public static void Update(string[] args)
        {
            LocalDBConfigInfo dbInfo = ReadLocalDBConfigFile("local_pages.txt");
            List<LocalDBPage> localDBPages = ReadLocalPages(dbInfo);
            if (localDBPages == null)
            {
                Console.WriteLine("Local database for pages is null, nothing to update ...");
                Console.WriteLine("Ending program ...");
                return;
            }

            MediawikiDBConfigInfo mwInfo = ReadMediawikiConfigFile("mediawiki.txt");
            List<LocalDBPage> mwPageDatas = ReadMediawikiPages(mwInfo);
            if (mwPageDatas == null)
            {
                Console.WriteLine("Mw_page database is null, nothing to update ...");
                Console.WriteLine("Ending program ...");
                return;
            }

            List<LocalDBPage> needsUpdate = (from x in localDBPages
                                             from y in mwPageDatas
                                             where (x.PageId == y.PageId) && (x.PageHash.ToUpper() != y.PageHash.ToUpper())
                                             select x).ToList();
        }

        /// <summary>
        /// This method updates all of given pages. This includes inserting new row into mw_text table and updating mw_page table (column page_latest).
        /// </summary>
        /// <param name="pages">list of pages that need to be updated.</param>
        /// <param name="mwInfo">config info for mediawiki database.</param>
        public static void UpdatePages(List<LocalDBPage> pages, MediawikiDBConfigInfo mwInfo)
        {

            foreach (LocalDBPage page in pages)
            {
                //najprv zavolaj insert do mw_text tabulky
                //teraz zistit index 
                int new_latest = FindMaxMwTextId(mwInfo.GetConnectionString(), mwInfo.MwTextTable, "old_id");
                //tu sprav update mw_page
            }
        }

        /// <summary>
        /// Method finds the highest index in the given table based on given column name.
        /// </summary>
        /// <param name="connectionString">string needed for connecting to table</param>
        /// <param name="tableName">the table we want to connect to</param>
        /// <param name="column">the name of column where ids are stalled</param>
        /// <returns>Method returns value of highest index in the table.</returns>
        static int FindMaxMwTextId(string connectionString, string tableName, string column)
        {
            string query = String.Format("SELECT * FROM {0}", tableName);

            // Prepare the connection
            MySqlConnection databaseConnection = new MySqlConnection(connectionString);
            MySqlCommand commandDatabase = new MySqlCommand(query, databaseConnection);
            commandDatabase.CommandTimeout = 60;
            MySqlDataReader reader;

            int max = 0;

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
                        int id = Convert.ToInt32(reader[column]);
                        if (id > max)
                        {
                            max = id;
                        }
                        
                    }
                }
                else
                {
                    Console.WriteLine("No rows found.");
                    return 0;
                }

                // Finally close the connection
                databaseConnection.Close();
                return max;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                throw new MwPageTableConnectionException(ex.Message);
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
