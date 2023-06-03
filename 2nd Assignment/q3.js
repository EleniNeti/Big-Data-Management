db.getCollection('Netflix').aggregate(
    [
      { $unwind: { path: '$listed_in' } },
      {
        $group: {
          _id: '$listed_in',
          totalSum: { $sum: 1 },
          movies: {
            $sum: {
              $cond: [
                { $eq: ['$type', 'Movie'] },
                1,
                0
              ]
            }
          },
          TVShows: {
            $sum: {
              $cond: [
                { $eq: ['$type', 'TV Show'] },
                1,
                0
              ]
            }
          }
        }
      },
      { $sort: { totalSum: -1 } },
      {
        $project: {
          _id: 0,
          genre: '$_id',
          totalSum: {
            $add: ['$movies', '$TVShows']
          }
        }
      },
      { $limit: 20 }
    ],
    { maxTimeMS: 60000, allowDiskUse: true }
  );