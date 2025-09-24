
# SqlServerCrud (VB.NET)

A lightweight, dependency-free ADO.NET helper for SQL Server with generic CRUD, stored procedures, paging, and retry logic. Written in **Visual Basic**.

## Features
- Generic CRUD with safe parameterization
- Sync & Async APIs
- Optional retry (deadlocks, timeouts)
- Stored procedure helpers
- Paging (OFFSET/FETCH)
- Transaction wrapper

## Quick Start
```vb
Dim db = New SqlServerCrud("Server=.;Database=MyDb;Trusted_Connection=True;")

Dim newId As Long = db.InsertAndReturnIdentity(Of Long)(
    "Users",
    New Dictionary(Of String, Object) From {
        {"FirstName","Tony"},
        {"LastName","Honesto"},
        {"Email","atonyhonesto@gmail.com"}
    },
    identityColumnName:="UserId")

Dim row = db.GetById("Users", New Dictionary(Of String, Object) From {{"UserId", newId}})
