db.getCollection('Netflix new ').aggregate(
  [
    {
      $project: {
        filteredListedIn: {
          $map: {
            input: '$listed_in',
            as: 'category',
            in: {
              $cond: [
                {
                  $regexMatch: {
                    input: '$duration',
                    regex: '^[0-9]+ min$'
                  }
                },
                '$$category',
                '$$REMOVE'
              ]
            }
          }
        },
        durationNumeric: {
          $toDouble: {
            $arrayElemAt: [
              { $split: ['$duration', ' '] },
              0
            ]
          }
        }
      }
    },
    { $unwind: '$filteredListedIn' },
    {
      $group: {
        _id: '$filteredListedIn',
        avgDuration: { $avg: '$durationNumeric' }
      }
    },
    { $sort: { avgDuration: -1, _id: 1 } },
    { $limit: 20 },
    {
      $project: {
        _id: 0,
        listed_in: '$_id',
        avgDuration: 1
      }
    }
  ],
  { maxTimeMS: 60000, allowDiskUse: true }
);
