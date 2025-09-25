Imports System.Data
Imports System.Data.SqlClient
Imports System.Text.RegularExpressions
Imports System.Threading
Imports System.Threading.Tasks

' ==========================================================================================
'  SqlServerCrud.vb
'
'  Lightweight SQL Server data access + generic CRUD helper for VB.NET (ADO.NET).
'  - Fully parameterized (no string concatenation of values)
'  - Sync & Async methods
'  - Optional retry policy for transient errors (e.g., deadlocks, timeouts)
'  - Stored procedure helpers
'  - Paging helper (OFFSET/FETCH)
'
'  QUICKSTART
'  ----------
'  Dim db = New SqlServerCrud("Server=.;Database=MyDb;Trusted_Connection=True;")
'
'  ' INSERT
'  Dim newId As Long = db.InsertAndReturnIdentity(Of Long)(
'      "Users",
'      New Dictionary(Of String, Object) From {{"FirstName","Tony"}, {"LastName","Honesto"}},
'      identityColumnName:="UserId")
'
'  ' SELECT
'  Dim userRow = db.GetById("Users", New Dictionary(Of String, Object) From {{"UserId", newId}})
'
'  ' UPDATE
'  db.Update("Users",
'            New Dictionary(Of String, Object) From {{"Email","atonyhonesto@gmail.com"}},
'            New Dictionary(Of String, Object) From {{"UserId", newId}})
'
'  ' DELETE
'  db.Delete("Users", New Dictionary(Of String, Object) From {{"UserId", newId}})
'
'  ' TRANSACTION (multiple statements commit/rollback together)
'  db.WithTransaction(Sub(tran)
'      db.Update("Accounts", New Dictionary(Of String, Object) From {{"Balance", 100D}},
'                New Dictionary(Of String, Object) From {{"AccountId", 1}}, tran)
'      db.Update("Accounts", New Dictionary(Of String, Object) From {{"Balance", 200D}},
'                New Dictionary(Of String, Object) From {{"AccountId", 2}}, tran)
'  End Sub)
'
'  ' STORED PROCEDURE
'  Dim outParams = New Dictionary(Of String, Object) From {{"@Total", DBNull.Value}}
'  Dim dt = db.ExecuteProcedureToTable("dbo.ListUsersByRole",
'                                      New Dictionary(Of String, Object) From {{"@Role","Admin"}},
'                                      outParams)
'
'  ' PAGED QUERY
'  Dim page1 = db.SelectPaged("Users", selectColumns:="UserId, FirstName, LastName",
'                             whereEquals:=Nothing, orderBy:="UserId ASC",
'                             pageNumber:=1, pageSize:=50)
'
' ==========================================================================================
''' <summary>
'''   A small, dependency-free ADO.NET helper for SQL Server with generic CRUD, stored proc,
'''   paging, and optional retry policy. Intended for simple apps, services, and admin tools.
''' </summary>
Public Class SqlServerCrud
    Private ReadOnly _connectionString As String
    Private ReadOnly _commandTimeout As Integer

    ' ---- Retry policy settings ----
    Private ReadOnly _maxRetries As Integer
    Private ReadOnly _retryDelayMs As Integer
    Private ReadOnly _shouldRetry As Func(Of SqlException, Boolean)

    ''' <param name="connectionString">Standard SQL Server connection string.</param>
    ''' <param name="commandTimeoutSeconds">Command timeout in seconds. Default 30.</param>
    ''' <param name="maxRetries">Max transient retries (0 disables). Default 2.</param>
    ''' <param name="retryDelayMs">Delay between retries in milliseconds. Default 300.</param>
    ''' <param name="transientPredicate">
    '''   Optional custom predicate to detect transient SQL errors. If omitted, a reasonable
    '''   default checks for deadlocks (1205), timeouts (-2), and "transport-level" errors.
    ''' </param>
    Public Sub New(connectionString As String,
                   Optional commandTimeoutSeconds As Integer = 30,
                   Optional maxRetries As Integer = 2,
                   Optional retryDelayMs As Integer = 300,
                   Optional transientPredicate As Func(Of SqlException, Boolean) = Nothing)

        If String.IsNullOrWhiteSpace(connectionString) Then Throw New ArgumentException("Connection string is required.")
        _connectionString = connectionString
        _commandTimeout = Math.Max(1, commandTimeoutSeconds)
        _maxRetries = Math.Max(0, maxRetries)
        _retryDelayMs = Math.Max(0, retryDelayMs)

        _shouldRetry = If(transientPredicate,
            Function(ex)
                ' Basic transient detection: deadlock (1205), timeout (-2), connection/transport issues
                Return ex.Errors.Cast(Of SqlError)().Any(Function(e) e.Number = 1205 OrElse e.Number = -2 OrElse
                                                          e.Message.IndexOf("transport-level", StringComparison.OrdinalIgnoreCase) >= 0 OrElse
                                                          e.Message.IndexOf("connection was closed", StringComparison.OrdinalIgnoreCase) >= 0)
            End Function)
    End Sub

#Region "Connection / Health"
    ''' <summary>Opens and closes a connection to confirm the DB is reachable.</summary>
    Public Function TestConnection() As Boolean
        Try
            Using conn As New SqlConnection(_connectionString)
                conn.Open()
                Return conn.State = ConnectionState.Open
            End Using
        Catch
            Return False
        End Try
    End Function

    ''' <summary>
    ''' Runs multiple DB operations in a single transaction. Commit on success, rollback on error.
    ''' </summary>
    Public Sub WithTransaction(work As Action(Of SqlTransaction))
        Using conn As New SqlConnection(_connectionString)
            conn.Open()
            Using tran = conn.BeginTransaction()
                Try
                    work(tran)
                    tran.Commit()
                Catch
                    Try : tran.Rollback() : Catch : End Try
                    Throw
                End Try
            End Using
        End Using
    End Sub
#End Region

#Region "Generic CRUD (Sync)"
    ''' <summary>INSERTs a row. Returns affected rows.</summary>
    Public Function Insert(table As String,
                           data As IDictionary(Of String, Object),
                           Optional tran As SqlTransaction = Nothing) As Integer
        ValidateTableAndData(table, data)
        Dim cols = data.Keys.Select(AddressOf Bracket)
        Dim params = data.Keys.Select(Function(k) "@" & Sanitize(k))
        Dim sql = $"INSERT INTO {Bracket(table)} ({String.Join(", ", cols)}) VALUES ({String.Join(", ", params)});"
        Return ExecuteNonQuery(sql, ToParams(data), tran)
    End Function

    ''' <summary>
    ''' INSERTs a row and returns the identity (OUTPUT INSERTED.[col] if provided; else SCOPE_IDENTITY()).
    ''' </summary>
    Public Function InsertAndReturnIdentity(Of T)(table As String,
                                                  data As IDictionary(Of String, Object),
                                                  Optional identityColumnName As String = Nothing,
                                                  Optional tran As SqlTransaction = Nothing) As T
        ValidateTableAndData(table, data)
        Dim cols = data.Keys.Select(AddressOf Bracket)
        Dim params = data.Keys.Select(Function(k) "@" & Sanitize(k))

        Dim sql As String
        If Not String.IsNullOrWhiteSpace(identityColumnName) Then
            sql = $"INSERT INTO {Bracket(table)} ({String.Join(", ", cols)}) OUTPUT INSERTED.{Bracket(identityColumnName)} VALUES ({String.Join(", ", params)});"
        Else
            sql = $"INSERT INTO {Bracket(table)} ({String.Join(", ", cols)}) VALUES ({String.Join(", ", params)}); SELECT CAST(SCOPE_IDENTITY() AS BIGINT);"
        End If

        Return ExecuteScalar(Of T)(sql, ToParams(data), tran)
    End Function

    ''' <summary>UPDATEs a rowset using key columns in <paramref name="keyWhere"/>. Returns affected rows.</summary>
    Public Function Update(table As String,
                           setData As IDictionary(Of String, Object),
                           keyWhere As IDictionary(Of String, Object),
                           Optional tran As SqlTransaction = Nothing) As Integer
        ValidateTable(table)
        If setData Is Nothing OrElse setData.Count = 0 Then Throw New ArgumentException("setData cannot be empty.")
        If keyWhere Is Nothing OrElse keyWhere.Count = 0 Then Throw New ArgumentException("keyWhere cannot be empty.")

        Dim setPairs = setData.Keys.Select(Function(c) $"{Bracket(c)}=@set_{Sanitize(c)}")
        Dim wherePairs = keyWhere.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
        Dim sql = $"UPDATE {Bracket(table)} SET {String.Join(", ", setPairs)} WHERE {String.Join(" AND ", wherePairs)};"

        Dim p = New Dictionary(Of String, Object)(StringComparer.OrdinalIgnoreCase)
        For Each kv In setData
            p("@set_" & Sanitize(kv.Key)) = ToDb(kv.Value)
        Next
        For Each kv In keyWhere
            p("@w_" & Sanitize(kv.Key)) = ToDb(kv.Value)
        Next

        Return ExecuteNonQuery(sql, p, tran)
    End Function

    ''' <summary>DELETEs rows matching <paramref name="keyWhere"/>. Returns affected rows.</summary>
    Public Function Delete(table As String,
                           keyWhere As IDictionary(Of String, Object),
                           Optional tran As SqlTransaction = Nothing) As Integer
        ValidateTable(table)
        If keyWhere Is Nothing OrElse keyWhere.Count = 0 Then Throw New ArgumentException("keyWhere cannot be empty.")
        Dim wherePairs = keyWhere.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
        Dim sql = $"DELETE FROM {Bracket(table)} WHERE {String.Join(" AND ", wherePairs)};"
        Dim p = keyWhere.ToDictionary(Function(kv) "@w_" & Sanitize(kv.Key),
                                      Function(kv) ToDb(kv.Value),
                                      StringComparer.OrdinalIgnoreCase)
        Return ExecuteNonQuery(sql, p, tran)
    End Function

    ''' <summary>SELECTs a single row by key. Returns Nothing if not found.</summary>
    Public Function GetById(table As String,
                            keyWhere As IDictionary(Of String, Object),
                            Optional columns As String = "*",
                            Optional tran As SqlTransaction = Nothing) As DataRow
        Dim dt = GetMany(table, keyWhere, columns, tran)
        If dt.Rows.Count = 0 Then Return Nothing
        Return dt.Rows(0)
    End Function

    ''' <summary>SELECTs a rowset optionally filtered by equality <paramref name="keyWhere"/>.</summary>
    Public Function GetMany(table As String,
                            keyWhere As IDictionary(Of String, Object),
                            Optional columns As String = "*",
                            Optional tran As SqlTransaction = Nothing) As DataTable
        ValidateTable(table)
        Dim whereClause As String = ""
        Dim p As Dictionary(Of String, Object)

        If keyWhere IsNot Nothing AndAlso keyWhere.Count > 0 Then
            Dim wherePairs = keyWhere.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
            whereClause = " WHERE " & String.Join(" AND ", wherePairs)
            p = keyWhere.ToDictionary(Function(kv) "@w_" & Sanitize(kv.Key),
                                      Function(kv) ToDb(kv.Value),
                                      StringComparer.OrdinalIgnoreCase)
        Else
            p = New Dictionary(Of String, Object)(StringComparer.OrdinalIgnoreCase)
        End If

        Dim sql = $"SELECT {columns} FROM {Bracket(table)}{whereClause};"
        Return ExecuteDataTable(sql, p, tran)
    End Function

    ''' <summary>
    ''' Convenience: returns a single page of rows using OFFSET/FETCH. PageNumber starts at 1.
    ''' </summary>
    Public Function SelectPaged(table As String,
                                Optional selectColumns As String = "*",
                                Optional whereEquals As IDictionary(Of String, Object) = Nothing,
                                Optional orderBy As String = "1 ASC",
                                Optional pageNumber As Integer = 1,
                                Optional pageSize As Integer = 50,
                                Optional tran As SqlTransaction = Nothing) As DataTable
        ValidateTable(table)
        If pageNumber < 1 Then pageNumber = 1
        If pageSize < 1 Then pageSize = 50

        Dim p As New Dictionary(Of String, Object)(StringComparer.OrdinalIgnoreCase)
        Dim whereClause As String = ""

        If whereEquals IsNot Nothing AndAlso whereEquals.Count > 0 Then
            Dim preds = whereEquals.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
            whereClause = " WHERE " & String.Join(" AND ", preds)
            For Each kv In whereEquals
                p("@w_" & Sanitize(kv.Key)) = ToDb(kv.Value)
            Next
        End If

        ' NOTE: orderBy should be trusted/constructed by you; identifiers are not auto-escaped here
        Dim offset = (pageNumber - 1) * pageSize
        Dim sql = $"SELECT {selectColumns} FROM {Bracket(table)}{whereClause} ORDER BY {orderBy} OFFSET {offset} ROWS FETCH NEXT {pageSize} ROWS ONLY;"

        Return ExecuteDataTable(sql, p, tran)
    End Function
#End Region

#Region "Stored Procedures (Sync)"
    ''' <summary>Executes a stored procedure and returns the first result set as a DataTable.</summary>
    Public Function ExecuteProcedureToTable(procName As String,
                                            Optional inputParams As IDictionary(Of String, Object) = Nothing,
                                            Optional outputParams As IDictionary(Of String, Object) = Nothing,
                                            Optional tran As SqlTransaction = Nothing) As DataTable
        Dim dt As New DataTable()
        Dim action As Action(Of SqlCommand) =
            Sub(cmd)
                cmd.CommandType = CommandType.StoredProcedure
                AddAllParams(cmd, inputParams, outputParams)
                Using da As New SqlDataAdapter(cmd)
                    da.Fill(dt)
                End Using
                CaptureOutputParams(cmd, outputParams)
            End Sub

        If tran Is Nothing Then
            ExecuteWithRetry(
                Sub()
                    Using conn As New SqlConnection(_connectionString)
                        conn.Open()
                        Using cmd As SqlCommand = CreateCommand(procName, conn, Nothing, Nothing)
                            action(cmd)
                        End Using
                    End Using
                End Sub)
        Else
            ExecuteWithRetry(Sub()
                                 Using cmd As SqlCommand = CreateCommand(procName, tran.Connection, tran, Nothing)
                                     action(cmd)
                                 End Using
                             End Sub)
        End If
        Return dt
    End Function

    ''' <summary>Executes a stored procedure that returns a single scalar value.</summary>
    Public Function ExecuteProcedureScalar(Of T)(procName As String,
                                                 Optional inputParams As IDictionary(Of String, Object) = Nothing,
                                                 Optional outputParams As IDictionary(Of String, Object) = Nothing,
                                                 Optional tran As SqlTransaction = Nothing) As T
        Dim result As Object = Nothing
        Dim action As Action(Of SqlCommand) =
            Sub(cmd)
                cmd.CommandType = CommandType.StoredProcedure
                AddAllParams(cmd, inputParams, outputParams)
                result = cmd.ExecuteScalar()
                CaptureOutputParams(cmd, outputParams)
            End Sub

        If tran Is Nothing Then
            ExecuteWithRetry(
                Sub()
                    Using conn As New SqlConnection(_connectionString)
                        conn.Open()
                        Using cmd As SqlCommand = CreateCommand(procName, conn, Nothing, Nothing)
                            action(cmd)
                        End Using
                    End Using
                End Sub)
        Else
            ExecuteWithRetry(Sub()
                                 Using cmd As SqlCommand = CreateCommand(procName, tran.Connection, tran, Nothing)
                                     action(cmd)
                                 End Using
                             End Sub)
        End If

        If result Is Nothing OrElse result Is DBNull.Value Then Return Nothing
        Return CType(Convert.ChangeType(result, GetType(T)), T)
    End Function
#End Region

#Region "General Purpose Commands (Sync)"
    ''' <summary>Executes a non-query (INSERT/UPDATE/DELETE) and returns affected rows.</summary>
    Public Function ExecuteNonQuery(sql As String,
                                    params As IDictionary(Of String, Object),
                                    Optional tran As SqlTransaction = Nothing) As Integer
        Dim affected As Integer = 0
        Dim action As Action(Of SqlCommand) =
            Sub(cmd) affected = cmd.ExecuteNonQuery()

        ExecuteText(sql, params, tran, action)
        Return affected
    End Function

    ''' <summary>Executes a scalar query and converts the result to T.</summary>
    Public Function ExecuteScalar(Of T)(sql As String,
                                        params As IDictionary(Of String, Object),
                                        Optional tran As SqlTransaction = Nothing) As T
        Dim result As Object = Nothing
        Dim action As Action(Of SqlCommand) =
            Sub(cmd) result = cmd.ExecuteScalar()

        ExecuteText(sql, params, tran, action)
        If result Is Nothing OrElse result Is DBNull.Value Then Return Nothing
        Return CType(Convert.ChangeType(result, GetType(T)), T)
    End Function

    ''' <summary>Executes a query and returns the first result set as a DataTable.</summary>
    Public Function ExecuteDataTable(sql As String,
                                     params As IDictionary(Of String, Object),
                                     Optional tran As SqlTransaction = Nothing) As DataTable
        Dim dt As New DataTable()
        Dim action As Action(Of SqlCommand) =
            Sub(cmd)
                Using da As New SqlDataAdapter(cmd)
                    da.Fill(dt)
                End Using
            End Sub

        ExecuteText(sql, params, tran, action)
        Return dt
    End Function
#End Region

#Region "Async API"
    ' Async versions mirror the sync ones. Useful for ASP.NET, services, etc.

    Public Async Function InsertAsync(table As String,
                                      data As IDictionary(Of String, Object),
                                      Optional tran As SqlTransaction = Nothing,
                                      Optional ct As CancellationToken = Nothing) As Task(Of Integer)
        ValidateTableAndData(table, data)
        Dim cols = data.Keys.Select(AddressOf Bracket)
        Dim params = data.Keys.Select(Function(k) "@" & Sanitize(k))
        Dim sql = $"INSERT INTO {Bracket(table)} ({String.Join(", ", cols)}) VALUES ({String.Join(", ", params)});"
        Return Await ExecuteNonQueryAsync(sql, ToParams(data), tran, ct)
    End Function

    Public Async Function InsertAndReturnIdentityAsync(Of T)(table As String,
                                                             data As IDictionary(Of String, Object),
                                                             Optional identityColumnName As String = Nothing,
                                                             Optional tran As SqlTransaction = Nothing,
                                                             Optional ct As CancellationToken = Nothing) As Task(Of T)
        ValidateTableAndData(table, data)
        Dim cols = data.Keys.Select(AddressOf Bracket)
        Dim params = data.Keys.Select(Function(k) "@" & Sanitize(k))

        Dim sql As String
        If Not String.IsNullOrWhiteSpace(identityColumnName) Then
            sql = $"INSERT INTO {Bracket(table)} ({String.Join(", ", cols)}) OUTPUT INSERTED.{Bracket(identityColumnName)} VALUES ({String.Join(", ", params)});"
        Else
            sql = $"INSERT INTO {Bracket(table)} ({String.Join(", ", cols)}) VALUES ({String.Join(", ", params)}); SELECT CAST(SCOPE_IDENTITY() AS BIGINT);"
        End If

        Return Await ExecuteScalarAsync(Of T)(sql, ToParams(data), tran, ct)
    End Function

    Public Async Function UpdateAsync(table As String,
                                      setData As IDictionary(Of String, Object),
                                      keyWhere As IDictionary(Of String, Object),
                                      Optional tran As SqlTransaction = Nothing,
                                      Optional ct As CancellationToken = Nothing) As Task(Of Integer)
        ValidateTable(table)
        If setData Is Nothing OrElse setData.Count = 0 Then Throw New ArgumentException("setData cannot be empty.")
        If keyWhere Is Nothing OrElse keyWhere.Count = 0 Then Throw New ArgumentException("keyWhere cannot be empty.")

        Dim setPairs = setData.Keys.Select(Function(c) $"{Bracket(c)}=@set_{Sanitize(c)}")
        Dim wherePairs = keyWhere.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
        Dim sql = $"UPDATE {Bracket(table)} SET {String.Join(", ", setPairs)} WHERE {String.Join(" AND ", wherePairs)};"

        Dim p = New Dictionary(Of String, Object)(StringComparer.OrdinalIgnoreCase)
        For Each kv In setData
            p("@set_" & Sanitize(kv.Key)) = ToDb(kv.Value)
        Next
        For Each kv In keyWhere
            p("@w_" & Sanitize(kv.Key)) = ToDb(kv.Value)
        Next

        Return Await ExecuteNonQueryAsync(sql, p, tran, ct)
    End Function

    Public Async Function DeleteAsync(table As String,
                                      keyWhere As IDictionary(Of String, Object),
                                      Optional tran As SqlTransaction = Nothing,
                                      Optional ct As CancellationToken = Nothing) As Task(Of Integer)
        ValidateTable(table)
        If keyWhere Is Nothing OrElse keyWhere.Count = 0 Then Throw New ArgumentException("keyWhere cannot be empty.")
        Dim wherePairs = keyWhere.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
        Dim sql = $"DELETE FROM {Bracket(table)} WHERE {String.Join(" AND ", wherePairs)};"
        Dim p = keyWhere.ToDictionary(Function(kv) "@w_" & Sanitize(kv.Key),
                                      Function(kv) ToDb(kv.Value),
                                      StringComparer.OrdinalIgnoreCase)
        Return Await ExecuteNonQueryAsync(sql, p, tran, ct)
    End Function

    Public Async Function GetByIdAsync(table As String,
                                       keyWhere As IDictionary(Of String, Object),
                                       Optional columns As String = "*",
                                       Optional tran As SqlTransaction = Nothing,
                                       Optional ct As CancellationToken = Nothing) As Task(Of DataRow)
        Dim dt = Await GetManyAsync(table, keyWhere, columns, tran, ct)
        If dt.Rows.Count = 0 Then Return Nothing
        Return dt.Rows(0)
    End Function

    Public Async Function GetManyAsync(table As String,
                                       keyWhere As IDictionary(Of String, Object),
                                       Optional columns As String = "*",
                                       Optional tran As SqlTransaction = Nothing,
                                       Optional ct As CancellationToken = Nothing) As Task(Of DataTable)
        ValidateTable(table)
        Dim whereClause As String = ""
        Dim p As Dictionary(Of String, Object)

        If keyWhere IsNot Nothing AndAlso keyWhere.Count > 0 Then
            Dim wherePairs = keyWhere.Keys.Select(Function(c) $"{Bracket(c)}=@w_{Sanitize(c)}")
            whereClause = " WHERE " & String.Join(" AND ", wherePairs)
            p = keyWhere.ToDictionary(Function(kv) "@w_" & Sanitize(kv.Key),
                                      Function(kv) ToDb(kv.Value),
                                      StringComparer.OrdinalIgnoreCase)
        Else
            p = New Dictionary(Of String, Object)(StringComparer.OrdinalIgnoreCase)
        End If

        Dim sql = $"SELECT {columns} FROM {Bracket(table)}{whereClause};"
        Return Await ExecuteDataTableAsync(sql, p, tran, ct)
    End Function

    Public Async Function ExecuteNonQueryAsync(sql As String,
                                               params As IDictionary(Of String, Object),
                                               Optional tran As SqlTransaction = Nothing,
                                               Optional ct As CancellationToken = Nothing) As Task(Of Integer)
        Dim affected As Integer = 0
        Dim func As Func(Of SqlCommand, Task) =
            Async Function(cmd)
                affected = Await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(False)
            End Function

        Await ExecuteTextAsync(sql, params, tran, func, ct).ConfigureAwait(False)
        Return affected
    End Function

    Public Async Function ExecuteScalarAsync(Of T)(sql As String,
                                                   params As IDictionary(Of String, Object),
                                                   Optional tran As SqlTransaction = Nothing,
                                                   Optional ct As CancellationToken = Nothing) As Task(Of T)
        Dim result As Object = Nothing
        Dim func As Func(Of SqlCommand, Task) =
            Async Function(cmd)
                result = Await cmd.ExecuteScalarAsync(ct).ConfigureAwait(False)
            End Function

        Await ExecuteTextAsync(sql, params, tran, func, ct).ConfigureAwait(False)
        If result Is Nothing OrElse result Is DBNull.Value Then Return Nothing
        Return CType(Convert.ChangeType(result, GetType(T)), T)
    End Function

    Public Async Function ExecuteDataTableAsync(sql As String,
                                                params As IDictionary(Of String, Object),
                                                Optional tran As SqlTransaction = Nothing,
                                                Optional ct As CancellationToken = Nothing) As Task(Of DataTable)
        Dim dt As New DataTable()
        Dim func As Func(Of SqlCommand, Task) =
            Async Function(cmd)
                Using reader = Await cmd.ExecuteReaderAsync(ct).ConfigureAwait(False)
                    dt.Load(reader)
                End Using
            End Function

        Await ExecuteTextAsync(sql, params, tran, func, ct).ConfigureAwait(False)
        Return dt
    End Function

    Public Async Function ExecuteProcedureToTableAsync(procName As String,
                                                       Optional inputParams As IDictionary(Of String, Object) = Nothing,
                                                       Optional outputParams As IDictionary(Of String, Object) = Nothing,
                                                       Optional tran As SqlTransaction = Nothing,
                                                       Optional ct As CancellationToken = Nothing) As Task(Of DataTable)
        Dim dt As New DataTable()
        Dim func As Func(Of SqlCommand, Task) =
            Async Function(cmd)
                cmd.CommandType = CommandType.StoredProcedure
                AddAllParams(cmd, inputParams, outputParams)
                Using reader = Await cmd.ExecuteReaderAsync(ct).ConfigureAwait(False)
                    dt.Load(reader)
                End Using
                CaptureOutputParams(cmd, outputParams)
            End Function

        Await ExecuteCommandAsync(procName, Nothing, tran, func, ct).ConfigureAwait(False)
        Return dt
    End Function

    Public Async Function ExecuteProcedureScalarAsync(Of T)(procName As String,
                                                            Optional inputParams As IDictionary(Of String, Object) = Nothing,
                                                            Optional outputParams As IDictionary(Of String, Object) = Nothing,
                                                            Optional tran As SqlTransaction = Nothing,
                                                            Optional ct As CancellationToken = Nothing) As Task(Of T)
        Dim result As Object = Nothing
        Dim func As Func(Of SqlCommand, Task) =
            Async Function(cmd)
                cmd.CommandType = CommandType.StoredProcedure
                AddAllParams(cmd, inputParams, outputParams)
                result = Await cmd.ExecuteScalarAsync(ct).ConfigureAwait(False)
                CaptureOutputParams(cmd, outputParams)
            End Function

        Await ExecuteCommandAsync(procName, Nothing, tran, func, ct).ConfigureAwait(False)
        If result Is Nothing OrElse result Is DBNull.Value Then Return Nothing
        Return CType(Convert.ChangeType(result, GetType(T)), T)
    End Function
#End Region

#Region "Low-level plumbing"
    Private Sub ExecuteText(sql As String,
                            params As IDictionary(Of String, Object),
                            tran As SqlTransaction,
                            action As Action(Of SqlCommand))
        If tran Is Nothing Then
            ExecuteWithRetry(
                Sub()
                    Using conn As New SqlConnection(_connectionString)
                        conn.Open()
                        Using cmd = CreateCommand(sql, conn, Nothing, params)
                            action(cmd)
                        End Using
                    End Using
                End Sub)
        Else
            ExecuteWithRetry(Sub()
                                 Using cmd = CreateCommand(sql, tran.Connection, tran, params)
                                     action(cmd)
                                 End Using
                             End Sub)
        End If
    End Sub

    Private Async Function ExecuteTextAsync(sql As String,
                                            params As IDictionary(Of String, Object),
                                            tran As SqlTransaction,
                                            func As Func(Of SqlCommand, Task),
                                            ct As CancellationToken) As Task
        If tran Is Nothing Then
            Await ExecuteWithRetryAsync(
                Async Function()
                    Using conn As New SqlConnection(_connectionString)
                        Await conn.OpenAsync(ct).ConfigureAwait(False)
                        Using cmd = CreateCommand(sql, conn, Nothing, params)
                            Await func(cmd).ConfigureAwait(False)
                        End Using
                    End Using
                End Function, ct).ConfigureAwait(False)
        Else
            Await ExecuteWithRetryAsync(
                Async Function()
                    Using cmd = CreateCommand(sql, tran.Connection, tran, params)
                        Await func(cmd).ConfigureAwait(False)
                    End Using
                End Function, ct).ConfigureAwait(False)
        End If
    End Function

    Private Async Function ExecuteCommandAsync(commandText As String,
                                               params As IDictionary(Of String, Object),
                                               tran As SqlTransaction,
                                               func As Func(Of SqlCommand, Task),
                                               ct As CancellationToken) As Task
        If tran Is Nothing Then
            Await ExecuteWithRetryAsync(
                Async Function()
                    Using conn As New SqlConnection(_connectionString)
                        Await conn.OpenAsync(ct).ConfigureAwait(False)
                        Using cmd = CreateCommand(commandText, conn, Nothing, params)
                            Await func(cmd).ConfigureAwait(False)
                        End Using
                    End Using
                End Function, ct).ConfigureAwait(False)
        Else
            Await ExecuteWithRetryAsync(
                Async Function()
                    Using cmd = CreateCommand(commandText, tran.Connection, tran, params)
                        Await func(cmd).ConfigureAwait(False)
                    End Using
                End Function, ct).ConfigureAwait(False)
        End If
    End Function

    Private Function CreateCommand(sql As String,
                                   conn As SqlConnection,
                                   tran As SqlTransaction,
                                   params As IDictionary(Of String, Object)) As SqlCommand
        Dim cmd As New SqlCommand(sql, conn) With {
            .CommandType = CommandType.Text,
            .CommandTimeout = _commandTimeout
        }
        If tran IsNot Nothing Then cmd.Transaction = tran
        If params IsNot Nothing Then
            For Each kvp In params
                cmd.Parameters.AddWithValue(kvp.Key, If(kvp.Value, DBNull.Value))
            Next
        End If
        Return cmd
    End Function

    Private Sub AddAllParams(cmd As SqlCommand,
                             inputParams As IDictionary(Of String, Object),
                             outputParams As IDictionary(Of String, Object))
        If inputParams IsNot Nothing Then
            For Each kvp In inputParams
                cmd.Parameters.AddWithValue(kvp.Key, If(kvp.Value, DBNull.Value))
            Next
        End If
        If outputParams IsNot Nothing Then
            For Each kvp In outputParams
                Dim p = cmd.Parameters.Add(kvp.Key, SqlDbType.Variant) ' type can be adjusted by caller later if needed
                p.Direction = ParameterDirection.Output
            Next
        End If
    End Sub

    Private Sub CaptureOutputParams(cmd As SqlCommand, outputParams As IDictionary(Of String, Object))
        If outputParams Is Nothing Then Return
        For Each p As SqlParameter In cmd.Parameters
            If p.Direction = ParameterDirection.Output OrElse p.Direction = ParameterDirection.InputOutput Then
                outputParams(p.ParameterName) = If(p.Value, DBNull.Value)
            End If
        Next
    End Sub
#End Region

#Region "Retry helpers"
    Private Sub ExecuteWithRetry(action As Action)
        Dim attempt = 0
        Do
            Try
                action()
                Return
            Catch ex As SqlException When attempt < _maxRetries AndAlso _shouldRetry(ex)
                Thread.Sleep(_retryDelayMs)
                attempt += 1
            End Try
        Loop
    End Sub

    Private Async Function ExecuteWithRetryAsync(action As Func(Of Task),
                                                 ct As CancellationToken) As Task
        Dim attempt = 0
        Do
            Try
                Await action().ConfigureAwait(False)
                Return
            Catch ex As SqlException When attempt < _maxRetries AndAlso _shouldRetry(ex)
                Await Task.Delay(_retryDelayMs, ct).ConfigureAwait(False)
                attempt += 1
            End Try
        Loop
    End Function
#End Region

#Region "Utilities"
    Private Shared Sub ValidateTable(table As String)
        If String.IsNullOrWhiteSpace(table) Then Throw New ArgumentException("Table is required.")
    End Sub

    Private Shared Sub ValidateTableAndData(table As String, data As IDictionary(Of String, Object))
        ValidateTable(table)
        If data Is Nothing OrElse data.Count = 0 Then Throw New ArgumentException("Data cannot be empty.")
    End Sub

    ''' <summary>Converts a name into a safe parameter token: letters/digits/underscore only.</summary>
    Private Shared Function Sanitize(name As String) As String
        Return Regex.Replace(name, "[^\w]", "_")
    End Function

    ''' <summary>Escapes identifiers and wraps them with [ ].</summary>
    Private Shared Function Bracket(identifier As String) As String
        Return "[" & identifier.Replace("]", "]]") & "]"
    End Function

    Private Shared Function ToParams(values As IDictionary(Of String, Object)) As IDictionary(Of String, Object)
        Dim p = New Dictionary(Of String, Object)(StringComparer.OrdinalIgnoreCase)
        For Each kv In values
            p("@" & Sanitize(kv.Key)) = ToDb(kv.Value)
        Next
        Return p
    End Function

    Private Shared Function ToDb(value As Object) As Object
        If value Is Nothing Then Return DBNull.Value
        Return value
    End Function
#End Region

End Class
