db.getCollection('Netflix').aggregate(
  [
    { $unwind: '$cast' },
    { $unwind: '$listed_in' },
    {
      $group: {
        _id: {
          cast: '$cast',
          genre: '$listed_in'
        },
        count: { $sum: 1 }
      }
    },
    { $sort: { '_id.cast': 1, count: -1 } },
    {
      $group: {
        _id: '$_id.cast',
        mostFrequentGenre: {
          $first: '$_id.genre'
        },
        count: { $first: '$count' }
      }
    },
    {
      $project: {
        _id: 0,
        name: '$_id',
        genre: '$mostFrequentGenre',
        count: 1
      }
    },
    { $sort: { name: 1, genre: 1, count: 1 } },
    { $limit: 20 }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);
