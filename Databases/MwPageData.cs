using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class MwPageData
    {
        public int PageId { get; set; }
        public byte[] PageTitle { get; set; }
        public int PageLatest { get; set; }

        public MwPageData()
        { 
        }

        public MwPageData(int id, byte[] title, int latest)
        {
            this.PageId = id;
            this.PageTitle = title;
            this.PageLatest = latest;
        }

        public string GetStringTitleName()
        {
            return ASCIIEncoding.ASCII.GetString(PageTitle);
        }
    }
}
