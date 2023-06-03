db.getCollection('Netflix').aggregate(
    [
      { $match: { type: 'TV Show' } },
      { $unwind: { path: '$country' } },
      {
        $group: {
          _id: '$country',
          count: { $sum: 1 }
        }
      },
      { $sort: { count: -1, _id: 1 } },
      {
        $project: {
          _id: 0,
          count: 1,
          country: '$_id'
        }
      },
      { $limit: 20 }
    ],
    { maxTimeMS: 60000, allowDiskUse: true }
  );