<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use App\Entity\SynchronisationInfo;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;

class InsertExampleCommand extends Command
{
    protected static $defaultName = 'app:insert-example';

    private $connection;
    private $httpClient;
    private $entityManager;
    private ContainerInterface $container;

    public function __construct(
        HttpClientInterface    $httpClient,
        EntityManagerInterface $entityManager,
        Connection             $connection,
        ContainerInterface     $container
    )
    {
        parent::__construct();
        $this->httpClient = $httpClient;
        $this->entityManager = $entityManager;
        $this->connection = $connection;
        $this->container = $container;
    }

    private function getAllTableNames(): array
    {
        $sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME NOT IN ('synchronisation_info', 'uv_commandecab', 't_achatdemandeinternedet','ua_t_facturefrsdet')"; // Exclude these tables

        $stmt = $this->connection->prepare($sql);
        $result = $stmt->executeQuery();
//        , 'uv_commandecab'
        return $result->fetchAllAssociative();
    }

    private function retryHttpRequest($method, $url, $options = [], $retries = 3, $delay = 1000)
    {
        $attempt = 0;
        while ($attempt < $retries) {
            try {
                $response = $this->httpClient->request($method, $url, $options);
                if ($response->getStatusCode() === 200) {
                    return $response;
                }
            } catch (\Exception $e) {
                // Log the exception and retry
                echo "HTTP request failed: " . $e->getMessage();
            }
            $attempt++;
            usleep($delay * 1000); // Delay in milliseconds before retrying
        }
        throw new \Exception("Failed to fetch data after $retries retries.");
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            // Start a new SynchronisationInfo record
            $synchronisation = new SynchronisationInfo();
            $synchronisation->setDateStart(new \DateTime());
            $this->entityManager->persist($synchronisation);
            $this->entityManager->flush();

            $ugouvApi = $this->container->getParameter('ugouv_api');
            $tables = $this->getAllTableNames();

            foreach ($tables as $table) {
                $tableName = $table['TABLE_NAME'];
//                $tableName = 'articles';

                $output->writeln("Processing table $tableName");

                $moreData = true;
                while ($moreData) {
                    try {
                        // Fetch unsynchronized data from API with retry logic
                        $response = $this->retryHttpRequest('POST', $ugouvApi . '/api/local/data', [
                            'body' => ['requete' => "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT 100"],
                            'verify_peer' => false,
                            'verify_host' => false,
                        ]);

                        $data = $response->toArray();

                        if (!empty($data)) {
                            // Check if table has 'id' column or use primary key(s)
                            $primaryKey = $this->getIdOrPrimaryKey($tableName);
//                            dd($primaryKey);
                            if (!$primaryKey) {
                                throw new \Exception("Table $tableName does not contain an 'id' column or primary key.");
                            }
                            // Check if we are dealing with a composite primary key
                            if (is_array($primaryKey)) {
                                // Composite key case: build an array of composite key values
                                $columnIds = [];
                                foreach ($data as $row) {
                                    $compositeKey = [];

                                    foreach ($primaryKey as $keyColumn) {
                                        // Check if $keyColumn is an array or a string
                                        if (is_array($keyColumn) && isset($keyColumn['ColumnName'])) {
                                            // If $keyColumn is an array, use the 'ColumnName' key
                                            $compositeKey[] = $row[$keyColumn['ColumnName']];
                                        } else {
                                            // If $keyColumn is a string, use it directly
                                            $compositeKey[] = $row[$keyColumn];
                                        }
                                    }

                                    // Concatenate composite key values with a separator (e.g., a dash or comma)
                                    $columnIds[] = implode('-', $compositeKey);  // You can use a different separator if needed
                                }


                            } else {
                                // Single primary key case
                                $columnIds = array_column($data, $primaryKey);
                            }
//                            dd($primaryKey);

//                            $columnIdsString = implode(', ', $columnIds);
//                            dd($columnIds);//

                            // Perform upsert operation
                            $this->upsertDataIntoTable($data, $tableName, $primaryKey);

                            // Mark the data as synchronized via API, including the primary key in the request
                            $flagResponse = $this->retryHttpRequest('POST', $ugouvApi . '/api/local/flag', [
                                'body' => [
                                    'table' => $tableName,
                                    'ids' => $columnIds,
                                    'primary_key' => $primaryKey  // Send the primary key used for flagging
                                ],
                                'verify_peer' => false,
                                'verify_host' => false,
                            ]);
                            if ($flagResponse->getStatusCode() !== 200) {
                                throw new \Exception($flagResponse->getContent(false));
                            }
                        } else {
                            // No more data to process for this table
                            $moreData = false;
                        }

                    } catch (\Exception $e) {
                        $output->writeln('Error with table ' . $tableName . ': ' . $e->getMessage());
                        break; // Break the loop for this table if an error occurs
                    }

                    // Free memory and force garbage collection after each iteration
                    gc_collect_cycles();
                }
            }

            $output->writeln('Data synchronized successfully!');
            $this->updateSyncInfo($synchronisation, 'success', 'Data inserted/updated successfully!');
            return 0; // Success

        } catch (\Exception $e) {
            $output->writeln('An error occurred: ' . $e->getMessage());
            $this->updateSyncInfo($synchronisation, 'error', $e->getMessage());
            return 1; // Failure
        }
    }

    /**
     * Check if the table has an 'id' column, otherwise return the primary key(s)
     */
    private function getIdOrPrimaryKey(string $tableName): ?array
    {
        // Query to check if the 'id' column exists
        $sql = "SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '$tableName' 
            AND COLUMN_NAME = 'id' 
            AND TABLE_SCHEMA = 'ugouv';";  // Adjust the schema name if needed

        // Execute the query
        $stmt = $this->connection->prepare($sql);
        $result = $stmt->executeQuery();

        // Fetch a single result (returns the first column value or false if not found)
        $idColumn = $result->fetchOne();
//dd($idColumn);
        // Check if 'id' column exists
        if ($idColumn) {
            return ['id'];  // Return 'id' as an array for consistency in return type
        }
//dd("hello");

        // If 'id' does not exist, fetch the primary key(s)
        $primaryKeys = $this->getPrimaryKeys($tableName);
        // If the table has one or more primary keys, return them
        if (count($primaryKeys) > 0) {
            return $primaryKeys;  // Return the array of primary keys, even if it's composite
        }


        // If no primary keys found, return null
        return null;
    }



    /**
     * Get the primary key(s) of a table.
     */
    private function getPrimaryKeys(string $tableName): array
    {
//        dd($tableName);
        // Query to get primary keys from sys.indexes, sys.index_columns, sys.columns, and sys.tables
        $sql = "
        SELECT 
            c.name AS ColumnName
        FROM 
            sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
        WHERE 
            i.is_primary_key = 1
            AND t.name = '$tableName'";  // Use a named parameter for the table name

        // Prepare the query
        $stmt = $this->connection->prepare($sql);

        // Execute the query with the table name as a parameter
        $result = $stmt->executeQuery();

        // Fetch all the primary key columns
        return $result->fetchAllAssociative();
    }




    private function upsertDataIntoTable(array $data, string $tableName, array $primaryKey): void
    {
        $tableNameSchema = "ugouv" . '.' . $tableName;
        $this->connection->beginTransaction();
//dd($data);
        $lign = [];
        try {
            // Disable foreign key checks
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' NOCHECK CONSTRAINT ALL');

            // Get the column types for the table
            $columnTypes = $this->getColumnTypes($tableName, 'ugouv');  // Adjust the schema if needed

            foreach ($data as $row) {

                // Validate date fields or other column types
                foreach ($row as $column => $value) {
                    if ($columnTypes[$column] === 'datetime' || $columnTypes[$column] === 'date' || $columnTypes[$column] === 'datetime2') {
                        if (!$this->validateDate($value)) {
                            $row[$column] = null; // Set invalid date/datetime to null
                        }
                    }
                    // Continue processing without skipping anything
                }

                // Build the WHERE clause for primary key (single or composite)
                $whereConditions = [];
                if (isset($primaryKey[0]['ColumnName'])) {
                    // Composite key case
                    foreach ($primaryKey as $keyColumn) {
                        $columnName = $keyColumn['ColumnName'];
                        $whereConditions[] = "$columnName = " . $this->connection->quote($row[$columnName]);
                    }
                } else {
                    // Single key case
                    $primaryKeyColumn = $primaryKey[0];
                    $whereConditions[] = "$primaryKeyColumn = " . $this->connection->quote($row[$primaryKeyColumn]);
                }

                // Combine the conditions into the WHERE clause
                $whereClause = implode(' AND ', $whereConditions);

                // Check if the record already exists using the WHERE clause
                $existsQuery = $this->connection->createQueryBuilder()
                    ->select('*')  // You can change this to the primary key or other columns as needed
                    ->from($tableNameSchema)
                    ->where($whereClause)
                    ->executeQuery()
                    ->fetchOne();
//                dd($existsQuery);

                if ($existsQuery) {
                    // Update the record if it exists
                    $this->updateRecord($tableNameSchema, $row, $primaryKey);
                } else {
                    // Insert the record if it doesn't exist
                    $this->insertRecord($tableNameSchema, $row);
                }
            }

            // Re-enable foreign key constraints
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' CHECK CONSTRAINT ALL');
            $this->connection->commit();

        } catch (\Exception $e) {
//            dd($e->getMessage());
//    dd($lign);
            $this->connection->rollBack();
            throw $e;
        }
    }

    private function validateDate($date): bool
    {
        if ($date === null || $date === '' || $date === '0000-00-00' || $date === '0000-00-00 00:00:00') {
            return false; // Consider null, empty, '0000-00-00', or '0000-00-00 00:00:00' as invalid
        }
        return (bool)strtotime($date);
    }

    private function getColumnTypes(string $tableName, string $schema): array
    {
        $sql = "
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?
        ";

        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue(1, $tableName);
        $stmt->bindValue(2, $schema);

        return $stmt->executeQuery()->fetchAllKeyValue();
    }

    private function updateRecord(string $tableNameSchema, array $row, array $primaryKey): void
    {
        $qb = $this->connection->createQueryBuilder();
        $qb->update($tableNameSchema);

        // Set all columns except for the primary key(s)
        foreach ($row as $column => $value) {
            // Skip the primary key column(s) in the SET part of the SQL query
            if (!in_array($column, array_column($primaryKey, 'ColumnName')) && !in_array($column, $primaryKey)) {
                $qb->set($column, '?');
                $qb->setParameter(count($qb->getParameters()), $value);
            }
        }

        // Handle composite or single primary key in the WHERE clause
        if (isset($primaryKey[0]['ColumnName'])) {
            // Composite primary key case
            $conditions = [];
            foreach ($primaryKey as $keyColumn) {
                $columnName = $keyColumn['ColumnName'];
                $conditions[] = "$columnName = ?";
                $qb->setParameter(count($qb->getParameters()), $row[$columnName]);
            }
            $qb->where(implode(' AND ', $conditions));
        } else {
            // Single primary key case
            $primaryKeyColumn = $primaryKey[0];  // Assuming it's a single string like 'id'
            $qb->where("$primaryKeyColumn = ?");
            $qb->setParameter(count($qb->getParameters()), $row[$primaryKeyColumn]);
        }

        // Execute the query
        $qb->executeStatement();
    }


    private function insertRecord(string $tableNameSchema, array $row): void
    {
        $qb = $this->connection->createQueryBuilder();
        $qb->insert($tableNameSchema);

        foreach ($row as $column => $value) {
            $qb->setValue($column, '?');
            $qb->setParameter(count($qb->getParameters()), $value);
        }

        $qb->executeStatement();
    }


    private function updateSyncInfo(SynchronisationInfo $synchronisation, string $status, string $message): void
    {
        $synchronisation->setInfo($status);
        $synchronisation->setDateEnd(new \DateTime());
        $synchronisation->setMessage($message);
        $this->entityManager->flush();
    }
}