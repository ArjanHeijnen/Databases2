using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace DataBases2022
{
     class Gebruiker
    {
        public ObjectId _id { get; set; }
        public int Gebruiker_ID { get; set; }
        public string Gebruiker_Email { get; set; }
        public string Wachtwoord { get; set; }

        public Boolean lowerThen(int num, int numID)
        {
            return num < numID;
        }

    }
}
