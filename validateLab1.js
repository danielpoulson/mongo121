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
