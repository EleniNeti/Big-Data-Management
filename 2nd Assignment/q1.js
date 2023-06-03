db.getCollection('Netflix').aggregate(
    [
      {
        $match: {
          date_added: { $regex: '.*2019.*' }
        }
      },
      {
        $project: {
          _id: 0,
          show_id: 1,
          type: 1,
          title: 1
        }
      },
      { $limit: 20 }
    ],
    { maxTimeMS: 60000, allowDiskUse: true }
  );