[
  {
    $group: {
      _id: "$rating",
      count: { $sum: 1 },
    },
  },
  {
    $sort: {
      count: -1,
    },
  },
  {
    $limit: 20,
  },
  {
    $project: {
      _id: 0,
      rating: "$_id",
      count: 1,
    },
  },
]
