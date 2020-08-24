using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Databases
{
    class LocalDBPage
    {
        public int PageId { get; set; }
        public string PageTitle { get; set; }
        public string PageHash { get; set; }
        public byte[] PageContent { get; set; }

        public LocalDBPage(int id, string title, string hash, byte[] content)
        {
            this.PageId = id;
            this.PageTitle = title;
            this.PageHash = hash;
            this.PageContent = content;
        }
    }
}
