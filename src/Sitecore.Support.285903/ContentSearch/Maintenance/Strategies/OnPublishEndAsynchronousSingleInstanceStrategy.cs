using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.Eventing;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  [DataContract]
  public class OnPublishEndAsynchronousSingleInstanceStrategy : Sitecore.ContentSearch.Maintenance.Strategies.OnPublishEndAsynchronousSingleInstanceStrategy
  {
    public OnPublishEndAsynchronousSingleInstanceStrategy(string database)
      : base(database)
    {
    }

    public override void Run()
    {
      EventManager.RaiseQueuedEvents();

      var eventQueue = this.Database.RemoteEvents.Queue;

      if (eventQueue == null)
      {
        CrawlingLog.Log.Fatal(string.Format("Event Queue is empty. Returning."));
        return;
      }

      var initialIndex = this.Indexes.OrderBy(index => (index.Summary.LastUpdatedTimestamp ?? 0)).FirstOrDefault();
      long? minLastUpdatedTimestamp = 0;
      if (initialIndex != null)
      {
        minLastUpdatedTimestamp = initialIndex.Summary.LastUpdatedTimestamp;
      }

      var queue = this.ReadQueue(eventQueue, minLastUpdatedTimestamp);
      var data = this.PrepareIndexData(queue, this.Database);

      if (this.ContentSearchSettings.IsParallelIndexingEnabled())
      {
        this.ParallelForeachProxy.ForEach(this.Indexes, new ParallelOptions
        {
          TaskScheduler = TaskSchedulerManager.GetLimitedConcurrencyLevelTaskSchedulerForIndexing(this.Indexes.Count + 1)
        },
          index => base.Run(data, index));
      }
      else
      {
        foreach (ISearchIndex index in this.Indexes)
        {
          base.Run(data, index);
        }
      }
    }
  }
}