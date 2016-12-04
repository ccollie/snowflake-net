using System;
using System.Collections.Generic;
using NUnit.Framework;
using Snowflake.Net;

namespace Snowflake.Tests
{
    [TestFixture]
    public class IdWorkerFixture
    {
        private const long WorkerMask = 0x000000000001F000L;
        private const long DatacenterMask = 0x00000000003E0000L;
        private const ulong TimestampMask = 0xFFFFFFFFFFC00000UL;

        public class WakingIdWorker : IdWorker
        {
            private int _slept = 0;

            public WakingIdWorker(long workerId, long datacenterId, long sequence = 0) : base(workerId, datacenterId, sequence)
            {
            }

            protected override long TilNextMillis(long lastTimestamp)
            {
                _slept++;
 	             return base.TilNextMillis(lastTimestamp);
            } 

            public int Slept
            {
                get { return _slept; }
            }
        }

        public class StaticTimeWorker : IdWorker
        {
            private long _time = 1L;

            public StaticTimeWorker(long workerId, long datacenterId, long sequence = 0)
                : base(workerId, datacenterId, sequence)
            {
            }

            protected override long TimeGen()
            {
 	            return _time + IdWorker.Twepoch;
            }

            public long Time
            {
                get { return _time; }
                set { _time = value; }
            }
        }
 
        /// <summary>
        /// 如何获取ID，ID>0
        /// </summary>
        [Test]
        public void It_should_generate_an_id()
        {
            var worker = new IdWorker(1, 1);
            var v = worker.NextId();
            Assert.That(v, Is.GreaterThan(0));
        }

        [Test]
        public void It_should_return_an_accurate_timestamp()
        {

        }

        /// <summary>
        /// 判断WorkerId是否是自己设置的1
        /// </summary>
        [Test]
        public void It_should_return_the_correct_job_id()
        {
            var s = new IdWorker(1, 1);
            Assert.That(s.WorkerId, Is.EqualTo(1));
        }

        /// <summary>
        /// 判断DatacenterId是否是自己设置的1
        /// </summary>
        [Test]
        public void It_should_return_the_datacenter_id()
        {
            var s = new IdWorker(1, 1);
            Assert.That(s.DatacenterId, Is.EqualTo(1));
        }

        /// <summary>
        /// 判断是否是正确的workerId
        /// </summary>
        [Test]
        public void It_should_properly_mask_worker_id()
        {
            const int workerId = 0x1F;
            const int datacenterId = 0;
            var worker = new IdWorker(workerId, datacenterId);
            for (var i = 0; i < 1000; i++)
            {
                var id = worker.NextId();
                var expected = (id & WorkerMask) >> 12;
                Assert.That(workerId, Is.EqualTo(expected));
            }
        }

        /// <summary>
        /// 判断是否是正确的datacenterId
        /// </summary>
        [Test]
        public void It_should_properly_mask_the_datacenter_id()
        {
            const int workerId = 0x1F;
            const int datacenterId = 0;
            var worker = new IdWorker(workerId, datacenterId);
            for (var i = 0; i < 1000; i++)
            {
                var id = worker.NextId();
                var expected = (id & DatacenterMask) >> 17;
                Assert.That(datacenterId, Is.EqualTo(expected));
            }
        }

        /// <summary>
        /// 判断是否是正确的时间轴
        /// </summary>
        [Test]
        public void It_should_properly_mask_timestamp()
        {
            var worker = new IdWorker(31, 31);
            for (var i = 0; i < 100; i++)
            {
                var t = TimeExtensions.CurrentTimeMillis();
                using (TimeExtensions.StubCurrentTime(t))
                {
                    var id = worker.NextId();
                    var actual = (((ulong)id & TimestampMask) >> 22);
                    var expected = (t - IdWorker.Twepoch);
                    Assert.That(expected, Is.EqualTo(actual));   
                }
            }
        }

        /// <summary>
        /// 检测溢出
        /// </summary>
        [Test]
        public void It_should_roll_over_sequence_id()
        {
            // put a zero in the low bit so we can detect overflow from the sequence
            const int workerId = 4;
            const int datacenterId = 4;
            var worker = new IdWorker(workerId, datacenterId);
            const int startSequence = 0xFFFFFF-20;
            const int endSequence = 0xFFFFFF+20;
            worker.Sequence = startSequence;

            for (var i = startSequence; i <= endSequence; i++)
            {
                var id = worker.NextId();
                var actual = (id & WorkerMask) >> 12;
                Assert.That(actual, Is.EqualTo(workerId));
            } 
        }

        /// <summary>
        /// 检测ID是不是递增的
        /// </summary>
        [Test]
        public void It_should_generate_increasing_ids()
        {
            var worker = new IdWorker(1, 1);
            var lastId = 0L;
            for (var i = 0; i < 100; i++)
            {
                var id = worker.NextId();
                Assert.That(id, Is.GreaterThan(lastId)); 
                lastId = id;
            }
        }

        /// <summary>
        /// 快速生成1000000个ID看看耗时多少
        /// </summary>
        [Test]
        public void It_should_generate_1_million_ids_quickly()
        {
            var worker = new IdWorker(31, 31);
            var t = TimeExtensions.CurrentTimeMillis();
            for (int i = 0; i < 1000000; i++)
            {
                var id = worker.NextId();
            }
            var t2 = TimeExtensions.CurrentTimeMillis();
            Console.WriteLine("generated 1000000 ids in {0} ms, or {1} ids/second", t2 - t, 1000000000.0/(t2-t));
        }

        /// <summary>
        /// 同一毫秒翻转两次的测试
        /// </summary>
        [Test]
        public void It_should_sleep_if_we_rollover_twice_in_the_same_millisecond()
        {
            var worker = new WakingIdWorker(1, 1);
            var iter = new List<long>()
                           {
                               2, 2, 3
                           };
            int idx = 0;
            Func<long> timeFunc = ()=>
                {
                    var res = iter[idx++];
                    if (idx > iter.Count - 1) idx = 0;
                    return res;
                };

            using (TimeExtensions.StubCurrentTime(timeFunc))
            {
                worker.Sequence = 4095;
                worker.NextId();
                worker.Sequence = 4095;
                worker.NextId();
            }
            Assert.That(worker.Slept, Is.EqualTo(1));
        }

        /// <summary>
        /// 生成唯一ID测试
        /// </summary>
        [Test]
        public void It_should_generate_only_unique_ids()
        {
            var worker = new IdWorker(31, 31);
            var set = new HashSet<long>();
            const int N = 2000000;
            for (var i = 0; i < N; i++)
            {
                var id = worker.NextId();
                if (set.Contains(id))
                {
                    Console.WriteLine("Found duplicate : {0}", id);
                }
                else
                {
                    set.Add(id);
                }
            }
            Assert.That(set.Count, Is.EqualTo(N));
        }

        /// <summary>
        /// 生成的ID应该超过500亿
        /// </summary>
        [Test]
        public void It_should_generate_ids_over_50_billion()
        {
            var worker = new IdWorker(0, 0);
            var id = worker.NextId();
            Assert.That(id, Is.GreaterThan(50000000000L));
        }

        /// <summary>
        /// 唯一ID测试
        /// </summary>
        [Test]
        public void It_should_generate_only_unique_ids_even_when_time_goes_backward()
        {
            const long sequenceMask = -1L ^ (-1L << 12);
            var worker = new StaticTimeWorker(0, 0);

            // reported at https://github.com/twitter/snowflake/issues/6
            // first we generate 2 ids with the same time, so that we get the sequqence to 1
            Assert.That(worker.Sequence, Is.EqualTo(0));
            Assert.That(worker.Time, Is.EqualTo(1));
            var id1 = worker.NextId();

            Assert.That(id1 >> 22, Is.EqualTo(1)); 
            Assert.That(id1 & sequenceMask, Is.EqualTo(0));

            Assert.That(worker.Sequence, Is.EqualTo(0)); 
            Assert.That(worker.Time, Is.EqualTo(1));
            var id2 = worker.NextId();

            Assert.That(id2 >> 22, Is.EqualTo(1)); 
            Assert.That(id2 & sequenceMask, Is.EqualTo(1));

            //then we set the time backwards

            worker.Time = 0;
            Assert.That(worker.Sequence, Is.EqualTo(1));
            Assert.Throws<Exception>(() => worker.NextId());
            Assert.That(worker.Sequence, Is.EqualTo(1));  // this used to get reset to 0, which would cause conflicts

            worker.Time = 1;
            var id3 = worker.NextId();
            Assert.That(id3 >> 22, Is.EqualTo(1)); 
            Assert.That(id3 & sequenceMask, Is.EqualTo(2));
        }
    }
}
