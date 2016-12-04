using Snowflake.Net;
using System;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            var worker = new IdWorker(1, 1);
            long id = worker.NextId();
            Console.WriteLine($"生成的ID为：{id}，他的长度是：{id.ToString().Length}");
            Console.ReadKey();
        }
    }
}
