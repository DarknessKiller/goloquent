# Sequel Datastore ORM

Inspired by Laravel Eloquent and Google Cloud Datastore

This repo still under development. We accept any pull request. ^\_^

## Database Support

* [x] MySQL

## Installation

```bash
  // dependency
  $ go get -u github.com/go-sql-driver/mysql
  $ go get -u cloud.google.com/go/datastore

  $ go get -u github.com/si3nloong/goloquent
```

* **Import the library**

```go
  import "github.com/si3nloong/goloquent"
```

## Quick Start

### Connect to database

```go
    import "github.com/si3nloong/goloquent/db"
    // Connect to mysql, please refer to https://github.com/go-sql-driver/mysql#dsn-data-source-name
    conn, err := db.Open("mysql", db.Config{
        Username: "root",
        Database: "test",
    })
    defer conn.Close()
    if err != nil {
        panic("Connection error: ", err)
    }
```

#### User Table

```go
// User : User kind parent is Merchant
type User struct {
    Key             *datastore.Key `goloquent:"__key__"` // load table key
    Name            string
    CountryCode     string
    PhoneNumber     string
    Age             int64          `goloquent:",unsigned"`
    CreatedDateTime time.Time
    UpdatedDateTime time.Time
}

// Load : load property
func (x *User) Load() error {
	return nil
}

// Save : manipulate property
func (x *User) Save() (error) {
	return nil
}
```

### Helper

```go
    // StringPrimaryKey is to get key value in string form
    key := datastore.NameKey("User", "mjfFgYnxBS", nil)
    fmt.Println(goloquent.StringKey(key)) // "mjfFgYnxBS"

    key = datastore.IDKey("User", int64(2305297334603281546), nil)
    fmt.Println(goloquent.StringKey(key)) // "2305297334603281546"
```

### Create Record

```go
    import "github.com/si3nloong/goloquent/db"

    // Example
    user := new(User)
    user.Name = "Hello World"
    user.Age = 18

    // OR
    var user *User
    user.Name = "Hello World"
    user.Age = 18

    // Create without parent key
    if err := db.Create(user, nil); err != nil {
        log.Println(err) // fail to create record
    }

    // Create with parent key
    parentKey := datastore.NameKey("Parent", "value", nil)
    if err := db.Create(user, parentKey); err != nil {
        log.Println(err) // fail to create record
    }

    // Create with self generate key
    key := datastore.NameKey("User", "uniqueID", nil)
    if err := db.Create(user, key); err != nil {
        log.Println(err) // fail to create record
    }
```

### Upsert Record

```go
    import "github.com/si3nloong/goloquent/db"

    // Example
    user := new(User)
    user.Name = "Hello World"
    user.Age = 18

    // Update if key exists, else create the user record
    parentKey := datastore.NameKey("Parent", "value", nil)
    if err := db.Upsert(user, parentKey); err != nil {
        log.Println(err) // fail
    }

    // Upsert with self generate key
    key := datastore.NameKey("User", "uniqueID", nil)
    if err := db.Upsert(user, key); err != nil {
        log.Println(err) // fail
    }
```

### Retrieve Record

* **Get Single Record using Primary Key**

```go
    // Example
    primaryKey := datastore.IDKey("User", int64(2305297334603281546), nil)
    user := new(User)
    if err := db.Find(primaryKey, user); err != nil {
        log.Println(err) // error while retrieving record
    }

    if err := db.Where("Status", "=", "ACTIVE").
        Find(primaryKey, user); err != goloquent.ErrNoSuchEntity {
        // if no record found using primary key, error `ErrNoSuchEntity` will throw instead
        log.Println(err) // error while retrieving record
    }
```

* **Get Single Record**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example 1
    user := new(User)
    if err := db.First(user); err != nil {
        log.Println(err) // error while retrieving record
    }

    if user.Key != nil { // if have record
        fmt.Println("Have record")
    } else { // no record
        fmt.Println("Doesnt't have record")
    }

    // Example 2
    user := new(User)
    if err := db.Where("Email", "=", "admin@hotmail.com").First(user); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Example 3
    parentKey := datastore.IDKey("Parent", 1093, nil)
    user := new(User)
    if err := db.NewQuery().
        Ancestor(parentKey).
        Where("Age", "=", 22).
        Order("-CreatedDateTime").
        First(user); err != nil {
        log.Println(err) // error while retrieving record
    }
```

* **Get Multiple Record**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example 1
    users := new([]User)
    if err := db.Limit(10).Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Example 2
    users := new([]*User)
    if err := db.Where("Name", "=", "Hello World").Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Example 3
    users := new([]User)
    if err := db.
        Ancestor(parentKey).
        Where("Name", "=", "myz").
        Where("Age", "=", 22).
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }
```

* **Get Record with Ordering**

```go
    import "github.com/si3nloong/goloquent/db"
    // Ascending order
    users := new([]*User)
    if err := db.
        Order("CreatedDateTime").
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // Descending order
    if err := db.Table("User").
        Order("-CreatedDateTime").
        Get(users); err != nil {
        log.Println(err) // error while retrieving record
    }
```

* **Pagination Record**

```go
    import "github.com/si3nloong/goloquent/db"

    p := goloquent.Pagination{
        Limit:  10,
        Cursor: "", // pass the cursor that generate by the query so that it will display the next record
    }

    // Example
    users := new([]*User)
    if err := db.Ancestor(parentKey).
        Order("-CreatedDateTime").
        Paginate(&p, users); err != nil {
        log.Println(err) // error while retrieving record
    }

    // ***************** OR ********************
    p := &goloquent.Pagination{
        Limit:  10, // number of records in each page
        Cursor: "EhQKCE1lcmNoYW50EK3bueKni5eNIxIWCgxMb3lhbHR5UG9pbnQaBkZrUUc4eA", // pass the cursor to get next record set
    }

    users := new([]*User)
    if err := db.Ancestor(parentKey).
        Order("-CreatedDateTime").
        Paginate(p, users); err != nil {
        log.Println(err) // error while retrieving record
    }
```

### Delete Record

* **Delete using Primary Key**

```go
    import "github.com/si3nloong/goloquent/db"
    // Example
    if err := db.Delete(user); err != nil {
        log.Println(err) // fail to delete record
    }
```

* **Delete using Where statement**

```go
    // Delete user table record which account type not equal to "PREMIUM" or "MONTLY"
    if err := db.Table("User").
        Where("AccountType", "!=", []string{
            "PREMIUM", "MONTLY",
        }).
        Delete(); err != nil {
        log.Println(err) // fail to delete record
    }
```

### Transaction

```go
    // Example
    if err := db.RunInTransaction(func(txn *goloquent.Connection) error {
        user := new(User)

        if err := txn.Table("User").Create(user, nil); err != nil {
            return err // return any err to rollback the transaction
        }

        return nil // return nil to commit the transaction
    }); err != nil {
        log.Println(err)
    }
```

### MySQL Exclusive

* **Database Migration**

```go
    // Example
    user := new(User)
    if err := db.Table("User").Migrate(user); err != nil {
        log.Println(err)
    }
```

* **Unique Index**

```go
    // Create unique Index
    if err := db.Table("User").
        UniqueIndex("CountryCode", "PhoneNumber"); err != nil {
        log.Println(err)
    }

    // Drop Unique Index
    if err := db.Table("User").
        DropUniqueIndex("CountryCode", "PhoneNumber"); err != nil {
        log.Println(err)
    }

    // Drop PrimaryKey
    if err := db.Table("User").DropUniqueIndex("__key__"); err != nil {
        log.Println(err)
    }
```

* **Drop Database**

```go
    // This will throw error if table is not exist
    if err := db.Table("User").Drop(); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }

    // Drop table if exists
    if err := db.Table("User").DropIfExists(); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }
```

* **Drop Database**

```go
    // This will throw error if table is not exist
    if err := db.Table("User").Drop(); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }

    // Drop table if exists
    if err := db.Table("User").DropIfExists(); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }
```

* **Table Locking (only effective inside RunInTransaction)**

```go
    // Example
    userKey := datastore.IDKey("User", int64(4645436182170916864), nil)
    if err := db.RunInTransaction(func(txn *goloquent.Connection) error {
        user := new(User)

        if err := txn.Table("User").
            LockForUpdate(). // Lock record for update
            Find(userKey, user); err != nil {
            return err
        }

        if err := txn.Table("User").Update(user); err != nil {
            return err // return any err to rollback the transaction
        }

        return nil // return nil to commit the transaction
    }); err != nil {
        log.Println(err)
    }
```

* **Sum Column**

```go
    // Example
    n, err := db.Table("User").Sum("Age")
    if err != nil {
        log.Println(err) // error while sum record
    }
```

* **Filter Query**

```go
    // Update single record
    user := new(User)
    if err := db.Table("User").
        Where("Status", "IN", []string{"active", "pending"}).
        First(user); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }

    // Get record with like
    if err := db.Table("User").
        Where("Name", "LIKE", "%name%").
        First(user); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }
```

* **Update Query**

```go
    // Update single record
    user := new(User)
    user.Key = datastore.IDKey("User", 167393, nil)
    user.Name = "Test"
    if err := db.Table("User").Update(user); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }

    // Update multiple record
    if err := db.Table("User").
        Where("Age", ">", 10).
        Update(map[string]interface{}{
            "Name": "New Name",
            "Email": "email@gmail.com",
            "UpdatedDateTime": time.Now().UTC(),
        }); err != nil {
        log.Println(err) // error while retrieving record or record not found
    }
```

* **Extra Schema Option**

```go
type datetime struct {
    CreatedDateTime time.Time // `CreatedDateTime`
    UpdatedDateTime time.Time // `UpdatedDateTime`
}

// Fields may have a `goloquent:"name,options"` tag.
type User struct {
    Key     *datastore.Key `goloquent:"__key__"` // Primary Key
    Name    string `goloquent:",longtext"` // Using `TEXT` datatype instead of `VARCHAR(255)` by default
    Age     int    `goloquent:",unsigned"` // Unsigned option only applicable for int data type
    PhoneNumber string `goloquent:",nullable"`
    Email   string `goloquent:",unique"`   // Make column `Email` as unique field
    Extra   string `goloquent:"-"` // Skip this field to store in db
    DefaultAddress struct {
        AddressLine1 string // `DefaultAddress.AddressLine1`
        AddressLine2 string // `DefaultAddress.AddressLine2`
        PostCode     int    // `DefaultAddress.PostCode`
        City         string // `DefaultAddress.City`
        State        string // `DefaultAddress.State`
        Country      string
    } `goloquent:",flatten"` // Flatten the struct field
    datetime // Embedded struct
}
```

The supported data type are :

```go
- string
- int, int8, int16, int32 and int64 (signed integers)
- uint, uint8, uint16, uint32 and uint64
- bool
- float32 and float64
- []byte
- any type whose underlying type is one of the above predeclared types
- *datastore.Key
- datastore.GeoPoint
- goloquent.SoftDelete
- time.Time (pointer time is not support)
- structs whose fields are all valid value types
- pointers to any one of the above
- slices of any of the above
```

| Data Type               | Schema                    | Default Value       |
| :---------------------- | :------------------------ | :------------------ |
| \*datastore.Key         | varchar(50), varchar(512) |                     |
| datastore.GeoPoint      | varchar(50)               | {Lat: 0, Lng: 0}    |
| string                  | varchar(255)              | ""                  |
| []byte                  | mediumblob                |                     |
| bool                    | boolean                   | false               |
| float32                 | decimal(10,2)             | 0                   |
| float64                 | decimal(10,2)             | 0                   |
| int, int8, int16, int32 | int                       | 0                   |
| int64                   | big integer               | 0                   |
| slice or array          | text                      | ""                  |
| struct                  | text                      | ""                  |
| time.Time               | datetime                  | 0001-01-01 00:00:00 |

**$Key**, **$Parent** are reserved words, please avoid to use these words as your column name

[MIT License](https://github.com/si3nloong/goloquent/blob/master/LICENSE)
