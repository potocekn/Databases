using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class MwTextTableConnectionException: Exception
    {
        public MwTextTableConnectionException()
        {

        }

        public MwTextTableConnectionException(string msg)
            : base(String.Format("The exception message text: {0}", msg))
        {

        }
    }
}
