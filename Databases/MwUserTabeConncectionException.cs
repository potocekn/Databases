using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class MwUserTabeConncectionException: Exception
    {
        public MwUserTabeConncectionException()
        {

        }

        public MwUserTabeConncectionException(string msg)
            : base(String.Format("The exception message text: {0}", msg))
        {

        }
    }
}
