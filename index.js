const express = require("express")
const { MongoClient, ObjectId } = require("mongodb")

const app = express()
app.use(express.json())

const url = "mongodb://127.0.0.1:27017"
const client = new MongoClient(url)
let db

async function connectDB() {
  await client.connect()
  db = client.db("assignment8")
  console.log("MongoDB Connected")
}
connectDB()

// 1. Create explicit collection "books" with validation
app.post("/collection/books", async (req, res) => {
  await db.createCollection("books", {
    validator: { title: { $ne: null } }
  })
  res.send("books collection created")
})

// 2. Create implicit collection "authors"
app.post("/collection/authors", async (req, res) => {
  const result = await db.collection("authors").insertOne(req.body)
  res.json(result)
})

// 3. Create capped collection "logs"
app.post("/collection/logs/capped", async (req, res) => {
  await db.createCollection("logs", { capped: true, size: 1000000 })
  res.send("logs capped collection created")
})

// 4. Create index on books.title
app.post("/collection/books/index", async (req, res) => {
  await db.collection("books").createIndex({ title: 1 })
  res.send("index created")
})

// 5. Insert one book
app.post("/books", async (req, res) => {
  const result = await db.collection("books").insertOne(req.body)
  res.json(result)
})

// 6. Insert multiple books
app.post("/books/batch", async (req, res) => {
  const result = await db.collection("books").insertMany(req.body)
  res.json(result)
})

// 7. Insert new log
app.post("/logs", async (req, res) => {
  const result = await db.collection("logs").insertOne(req.body)
  res.json(result)
})

// 8. Update book "Future" year = 2022
app.patch("/books/Future", async (req, res) => {
  const result = await db.collection("books").updateOne(
    { title: "Future" },
    { $set: { year: 2022 } }
  )
  res.json(result)
})

// 9. Find book by title
app.get("/books/title", async (req, res) => {
  const book = await db.collection("books").findOne({ title: req.query.title })
  res.json(book)
})

// 10. Find books between years
app.get("/books/year", async (req, res) => {
  const { from, to } = req.query
  const books = await db.collection("books")
    .find({ year: { $gte: Number(from), $lte: Number(to) } })
    .toArray()
  res.json(books)
})

// 11. Find books by genre
app.get("/books/genre", async (req, res) => {
  const books = await db.collection("books")
    .find({ genres: req.query.genre })
    .toArray()
  res.json(books)
})

// 12. Skip 2, limit 3, sort desc
app.get("/books/skip-limit", async (req, res) => {
  const books = await db.collection("books")
    .find()
    .sort({ year: -1 })
    .skip(2)
    .limit(3)
    .toArray()
  res.json(books)
})

// 13. Year is integer
app.get("/books/year-integer", async (req, res) => {
  const books = await db.collection("books")
    .find({ year: { $type: "int" } })
    .toArray()
  res.json(books)
})

// 14. Exclude Horror & Science Fiction
app.get("/books/exclude-genres", async (req, res) => {
  const books = await db.collection("books")
    .find({ genres: { $nin: ["Horror", "Science Fiction"] } })
    .toArray()
  res.json(books)
})

// 15. Delete books before year
app.delete("/books/before-year", async (req, res) => {
  const result = await db.collection("books").deleteMany({
    year: { $lt: Number(req.query.year) }
  })
  res.json(result)
})

// 16. Aggregate after 2000 sorted desc
app.get("/books/aggregate1", async (req, res) => {
  const result = await db.collection("books").aggregate([
    { $match: { year: { $gt: 2000 } } },
    { $sort: { year: -1 } }
  ]).toArray()
  res.json(result)
})

// 17. Aggregate project fields
app.get("/books/aggregate2", async (req, res) => {
  const result = await db.collection("books").aggregate([
    { $match: { year: { $gt: 2000 } } },
    { $project: { _id: 0, title: 1, author: 1, year: 1 } }
  ]).toArray()
  res.json(result)
})

// 18. Unwind genres
app.get("/books/aggregate3", async (req, res) => {
  const result = await db.collection("books").aggregate([
    { $unwind: "$genres" }
  ]).toArray()
  res.json(result)
})

// 19. Join books with logs
app.get("/books/aggregate4", async (req, res) => {
  const result = await db.collection("books").aggregate([
    {
      $lookup: {
        from: "logs",
        localField: "_id",
        foreignField: "book_id",
        as: "logs"
      }
    }
  ]).toArray()
  res.json(result)
})

app.listen(3000, () => {
  console.log("Server running on port 3000")
})
