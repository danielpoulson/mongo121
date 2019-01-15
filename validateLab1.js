var pipeline2 = [
  {
    $match: {
      "imdb.rating": { $gte: 7 },
      genres: { $nin: ["Crime", "Horror"] },
      $or: [{ rated: "PG" }, { rated: "G" }],
      $and: [{ languages: "English" }, { languages: "Japanese" }]
    }
  },
  {
    $project: { title: 1, rated: 1, _id: 0 }
  }
];

var pipeline = [
  {
    $project: { title: { $size: { $split: ["$title", " "] } } }
  },
  {
    $match: { title: 1 }
  }
];

db.movies.aggregate([
  {
    $match: {
      cast: { $elemMatch: { $exists: true } },
      directors: { $elemMatch: { $exists: true } },
      writers: { $elemMatch: { $exists: true } }
    }
  },
  {
    $project: {
      _id: 0,
      cast: 1,
      directors: 1,
      writers: {
        $map: {
          input: "$writers",
          as: "writer",
          in: {
            $arrayElemAt: [
              {
                $split: ["$$writer", " ("]
              },
              0
            ]
          }
        }
      }
    }
  },
  {
    $project: {
      labor_of_love: {
        $gt: [
          { $size: { $setIntersection: ["$cast", "$directors", "$writers"] } },
          0
        ]
      }
    }
  },
  {
    $match: { labor_of_love: true }
  },
  {
    $count: "labors of love"
  }
]);

db.movies.aggregate([
  {
    $match: {
      "tomatoes.viewer.rating": { $gte: 3 },
      countries: { $in: ["USA"] },
      cast: {
        $in: [
          "Sandra Bullock",
          "Tom Hanks",
          "Julia Roberts",
          "Kevin Spacey",
          "George Clooney"
        ]
      }
    }
  },
  {
    $project: {
      _id: 0,
      title: 1,
      "tomatoes.viewer.rating": 1,
      countries: 1,
      cast: 1,
      num_favs: {
        $size: {
          $setIntersection: [
            "$cast",
            [
              "Sandra Bullock",
              "Tom Hanks",
              "Julia Roberts",
              "Kevin Spacey",
              "George Clooney"
            ]
          ]
        }
      }
    }
  },

  { $sort: { num_favs: -1, "tomatoes.viewer.rating": -1, title: -1 } },
  { $skip: 24 },
  { $limit: 3 }
]);

db.movies.aggregate([
  {
    $match: {
      "imdb.rating": { $gte: 1 },
      "imdb.votes": { $gte: 1 },
      languages: { $in: ["English"] },
      released: { $gte: ISODate("1990-01-01T00:00:00Z") }
    }
  },
  {
    $project: {
      title: 1,
      "imdb.votes": 1,
      "imdb.rating": 1,
      scaled_votes: {
        $add: [
          1,
          {
            $multiply: [
              9,
              {
                $divide: [
                  { $subtract: ["$imdb.votes", 5] },
                  { $subtract: [1521105, 5] }
                ]
              }
            ]
          }
        ]
      }
    }
  },
  {
    $addFields: {
      normalized_rating: { $avg: ["$scaled_votes", "$imdb.rating"] }
    }
  },
  { $sort: { normalized_rating: 1 } },
  { $limit: 1 }
]);
