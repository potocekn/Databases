using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class WrongPortFormatException : Exception
    {
        public WrongPortFormatException()
        {

        }

        public WrongPortFormatException(string msg)
            : base(String.Format("The exception message text: {0}", msg))
        {

        }
    }
}
