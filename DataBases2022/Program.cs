using System;
using System.Collections.Generic;
using System.Linq;
using System.Data.SqlClient;
using System.Diagnostics;
using MongoDB.Driver;
using MongoDB.Bson;

namespace DataBases2022
{
    class Program
    {

        static void Main(string[] args)
        {
            Boolean loop = true;
            while (loop)
            {
                Console.WriteLine("What database do you want to use? (ADO - mongo - none)");
                var databaseToUse = Console.ReadLine();
                switch (databaseToUse)
                {
                    case "ADO":
                        SqlConnection cnn;
                        string connetionString = "Data Source=LAPTOP-SDTI3940;Initial Catalog=NETFLIX;Integrated Security=True";
                        cnn = new SqlConnection(connetionString);
                        try
                        {
                            cnn.Open();
                            Console.WriteLine("Connection Open ! ");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine("Can not open connection ! ");
                        }
                        Console.WriteLine("What operation do you want to use? (c-r-u-d)");
                        var crud_op = Console.ReadLine();
                        switch (crud_op)
                        {
                            case "c":
                                cadoNET(cnn);
                                break;
                            case "r":
                                radoNET(cnn);
                                break;
                            case "u":
                                uadoNET(cnn);
                                break;
                            case "d":
                                dadoNET(cnn);
                                break;
                            default:
                                Console.WriteLine("Fill in Correctly.");
                                break;
                        }
                        cnn.Close();
                        break;
                    case "mongo":
                        MongoClient mClient = new MongoClient("mongodb+srv://arjan:test@cluster0.kx7yc.mongodb.net/?retryWrites=true&w=majority");
                        MongoDatabaseBase mDatabase = (MongoDatabaseBase)mClient.GetDatabase("Netflix");
                        MongoCollectionBase<Gebruiker> mCollection = (MongoCollectionBase<Gebruiker>)mDatabase.GetCollection<Gebruiker>("Gebruiker");

                        Console.WriteLine("What operation do you want to use? (c-r-u-d)");
                        var crud_op_m = Console.ReadLine();
                        switch (crud_op_m)
                        {
                            case "c":
                                cMongodb(mCollection);
                                break;
                            case "r":
                                rMongodb(mCollection);
                                break;
                            case "u":
                                uMongodb(mCollection);
                                break;
                            case "d":
                                dMongodb(mCollection);
                                break;
                            default:
                                Console.WriteLine("Fill in Correctly.");
                                break;
                        }
                        break;
                    case "eframe":
                        loop = false;
                        break;
                    case "none":
                        loop = false;
                        break;
                    default:
                        Console.WriteLine("Fill in Correct database name.");
                        break;

                }
                Console.WriteLine("Connection closed!");

            }
        }

        private static void dMongodb(MongoCollectionBase<Gebruiker> mCollection)
        {
            Console.WriteLine("How much do you want to delete? (max. 1,000,000)");
            int n = (int)Int64.Parse(Console.ReadLine());
            if (n <= 1000000)
            {
                Stopwatch stopWatch = new Stopwatch();
                var filter = Builders<Gebruiker>.Filter.Where(p => 0 >= p.Gebruiker_ID.CompareTo(n));
                stopWatch.Start();
                mCollection.DeleteMany(filter);

                stopWatch.Stop();
                showTime(stopWatch);
            }
        }

        private static void uMongodb(MongoCollectionBase<Gebruiker> mCollection)
        {
            Console.WriteLine("How much do you want to update? (max. 1,000,000)");
            int n = (int)Int64.Parse(Console.ReadLine());
            if (n <= 1000000)
            {
                Stopwatch stopWatch = new Stopwatch();     
                var filter = Builders<Gebruiker>.Filter.Where(p => 0 >= p.Gebruiker_ID.CompareTo(n));
                var update = Builders<Gebruiker>.Update.Set("Wachtwoord", "NoSecret");
                stopWatch.Start();
                UpdateResult updateResult = mCollection.UpdateMany(filter, update);

                stopWatch.Stop();
                showTime(stopWatch);
            }
        }

        private static void rMongodb(MongoCollectionBase<Gebruiker> mCollection)
        {
            Console.WriteLine("How much do you want to read? (max. 1,000,000)");
            int n = (int)Int64.Parse(Console.ReadLine());
            if (n <= 1000000)
            {
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                var mcol = mCollection.Find(new BsonDocument()).Limit(n).ToList();
                stopWatch.Stop();
                Console.WriteLine("found: " + mcol.Count);
                showTime(stopWatch);
            }

        }

        private static void cMongodb(MongoCollectionBase<Gebruiker> mCollection)
        {
            Console.WriteLine("How much do you want to insert? (max. 1,000,000)");
            int n = (int)Int64.Parse(Console.ReadLine());
            if (n <= 1000000)
            {
                List<Gebruiker> list = new List<Gebruiker>();
                for (int i = 1; i <= n; i++)
                {
                    Gebruiker gebruiker = new Gebruiker
                    {
                        Gebruiker_ID = i,
                        Gebruiker_Email = "fake@g-mail.com",
                        Wachtwoord = "Secret"
                    };
                    list.Add(gebruiker);
                }
                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                mCollection.InsertMany(list);
                stopWatch.Stop();
                showTime(stopWatch);
            }
        }

        public static void radoNET(SqlConnection cnn)
        {
            Console.WriteLine("How much do you want to select? (max. 1,000,000)");
            var amount = Int64.Parse(Console.ReadLine());
            if (amount <= 1000000)
            {
                int amnt = (int)amount;

                Stopwatch stopWatch = new Stopwatch();
                stopWatch.Start();
                SqlCommand cmd = new SqlCommand("select top (@amount) * from Gebruiker;", cnn);
                cmd.CommandType = System.Data.CommandType.Text;
                SqlParameter param = new SqlParameter();
                param.ParameterName = "@amount";
                param.Value = amnt;
                cmd.Parameters.Add(param);
                SqlDataReader rdr = cmd.ExecuteReader();
                int length = 0;
                while (rdr.Read())
                {
                    length += 1;
                }
                Console.WriteLine("Length: " + length);
                stopWatch.Stop();
                showTime(stopWatch);
            }
        }
        public static void cadoNET(SqlConnection cnn)
        {
            SqlCommand cmd = new SqlCommand("SELECT MAX(Gebruiker_ID) as max FROM Gebruiker", cnn);
            SqlDataReader rdr = cmd.ExecuteReader();
            int max = 0;
            while (rdr.Read())
            {
                max = (int)Int64.Parse(rdr["max"].ToString());

            }
            rdr.Close();
            Console.WriteLine("How much do you want to insert? (max. 1,000,000)");
            var amount = Int64.Parse(Console.ReadLine());
            if (amount <= 1000000)
            {
                Stopwatch stopWatch = new Stopwatch();
                int amnt = (int)amount;
                Boolean m = true;
                while (m)
                {
                    int t = 1000;
                    if (amnt <= 1000)
                    {
                        t = amnt;
                        m = false;
                    }
                    amnt = amnt - t;
                    string com = "insert into Gebruiker (Gebruiker_ID, Gebruiker_Email, Wachtwoord) Values";
                    for (int c = 1; c <= t; c++)
                    {
                        com += " (" + (max + c) + ", 'fake@mail.com', 'Secret'),";
                    }
                    com = com.Remove(com.Length - 1); ;
                    stopWatch.Start();
                    max = max + t;
                    SqlCommand cmd2 = new SqlCommand(com, cnn);
                    cmd2.ExecuteNonQuery();

                    stopWatch.Stop();
                    // Get the elapsed time as a TimeSpan value.
                }
                showTime(stopWatch);
            }
        }
        public static void uadoNET(SqlConnection cnn)
        {
            Console.WriteLine("How much do you want to update? (max. 1,000,000)");
            var amount = Int64.Parse(Console.ReadLine());
            if (amount <= 1000000)
            {
                Stopwatch stopWatch = new Stopwatch();
                int amnt = (int)amount;

                SqlCommand cmd = new SqlCommand(";WITH CTE AS (SELECT TOP(@amount) * FROM Gebruiker ORDER BY Gebruiker_ID) UPDATE CTE SET Geactiveerd = 2", cnn);

                cmd.CommandType = System.Data.CommandType.Text;
                SqlParameter param = new SqlParameter();
                param.ParameterName = "@amount";
                param.Value = amnt;
                cmd.Parameters.Add(param);

                stopWatch.Start();
                cmd.ExecuteNonQuery();

                stopWatch.Stop();
                // Get the elapsed time as a TimeSpan value.

                showTime(stopWatch);
            }
        }

        public static void dadoNET(SqlConnection cnn)
        {
            Console.WriteLine("How much do you want to delete? (max. 1,000,000)");
            var amount = Int64.Parse(Console.ReadLine());
            if (amount <= 1000000)
            {
                Stopwatch stopWatch = new Stopwatch();
                int amnt = (int)amount;

                SqlCommand cmd = new SqlCommand("WITH CTE AS (SELECT TOP(@amount) * FROM Gebruiker ORDER BY Gebruiker_ID DESC) Delete CTE", cnn);

                cmd.CommandType = System.Data.CommandType.Text;
                SqlParameter param = new SqlParameter();
                param.ParameterName = "@amount";
                param.Value = amnt;
                cmd.Parameters.Add(param);

                stopWatch.Start();
                cmd.ExecuteNonQuery();

                stopWatch.Stop();
                // Get the elapsed time as a TimeSpan value.

                showTime(stopWatch);
            }
        }
        private static void showTime(Stopwatch stopWatch)
        {
            TimeSpan ts = stopWatch.Elapsed;

            // Format and display the TimeSpan value.
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
                ts.Hours, ts.Minutes, ts.Seconds,
                ts.Milliseconds / 10);
            Console.WriteLine("RunTime " + elapsedTime);
        }


    }




}
