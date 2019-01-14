using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Sitecore.ContentSearch;
using Sitecore.ContentSearch.Diagnostics;
using Sitecore.ContentSearch.Maintenance.Strategies;
using Sitecore.Eventing;
using Sitecore.Jobs;

namespace Sitecore.Support.ContentSearch.Maintenance.Strategies
{
  /// <summary>
  /// The on publish end asynchronous single instance strategy.
  /// </summary>
  [DataContract]
  public class OnPublishEndAsynchronousSingleInstanceStrategy : OnPublishEndAsynchronousStrategy
  {
    /// <summary>
    /// Inherits the class <see cref="BaseAsynchronousStrategy"/> class.
    /// </summary>
    /// <param name="database">
    /// The database.
    /// </param>
    public OnPublishEndAsynchronousSingleInstanceStrategy(string database)
      : base(database)
    {
    }

    /// <summary>
    /// Runs the pipeline.
    /// </summary>
    public override void Run()
    {
      EventManager.RaiseQueuedEvents(); // in order to prevent indexing out-of-date data we have to force processing queued events before reading queue.

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
          TaskScheduler = TaskSchedulerManager.LimitedConcurrencyLevelTaskSchedulerForIndexing
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