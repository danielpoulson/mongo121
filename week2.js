db.movies.aggregate([
  {
    $match: {
      awards: /^Won /
    }
  },
  { $group: { _id: null, count: { $sum: 1 } } }
]);

db.movies.aggregate([
  {
    $match: {
      awards: /^Won /
    }
  },
  { $limit: 2 }
]);
