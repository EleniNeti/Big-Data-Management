db.getCollection('Netflix new ').aggregate(
  [
    { $unwind: '$director' },
    {
      $group: {
        _id: '$director',
        count: { $sum: 1 }
      }
    },
    { $sort: { count: -1 } },
    { $limit: 20 },
    {
      $project: {
        _id: 0,
        director: '$_id',
        count: 1
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);