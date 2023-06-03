db.getCollection('Netflix').aggregate(
  [
    { $unwind: '$cast' },
    {
      $group: { _id: '$cast', count: { $sum: 1 } }
    },
    { $sort: { count: -1, _id: 1 } },
    { $limit: 20 },
    {
      $project: { _id: 0, name: '$_id', count: 1 }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);
