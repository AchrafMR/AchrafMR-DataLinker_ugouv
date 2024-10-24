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
                AND TABLE_NAME NOT IN ('synchronisation_info', 'uv_commandecab')"; // Exclude these tables

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
                $output->writeln("Processing table $tableName");

                $moreData = true;
                while ($moreData) {

                    // Begin a transaction for each batch of data
//                    $this->connection->beginTransaction();
                    try {


                        // Fetch unsynchronized data from API with retry logic
                        $response = $this->retryHttpRequest('POST', $ugouvApi . '/api/local/data', [
                            'body' => ['requete' => "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT 100"],
                            'verify_peer' => false,
                            'verify_host' => false,
                        ]);

                        $data = $response->toArray();

                        if (!empty($data)) {
                            if (!isset($data[0]['id'])) {
                                $output->writeln("Table $tableName does not contain an 'id' key.");
                                $moreData = false;
                            }

                            // Extract primary key values
                            $columnIds = array_column($data, 'id');
                            $columnIdsString = implode(', ', $columnIds);

                            // Perform upsert operation
                            $this->upsertDataIntoTable($data, $tableName);
                            // Mark the data as synchronized via API
                            $flagResponse = $this->retryHttpRequest('POST', $ugouvApi . '/api/local/flag', [
                                'body' => [
                                    'table' => $tableName,
                                    'ids' => $columnIdsString,
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
//                        $this->connection->commit();



                    } catch (\Exception $e) {
//                        $this->connection->rollBack();
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


    private function upsertDataIntoTable(array $data, string $tableName): void
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
                $lign = $row;
                $columns = array_keys($row);
                $primaryKey = 'id'; // Assuming 'id' is the primary key

                // Validate date fields or other column types
                foreach( $row as $column => $value ) {
                    if ($columnTypes[$column] === 'datetime' || $columnTypes[$column] === 'date' || $columnTypes[$column] === 'datetime2') {
                        if (!$this->validateDate($value)) {
                            $row[$column] = null; // Set invalid date/datetime to null
                        }
                    }
                    // Continue processing without skipping anything
                }

                // Check if the record already exists
                $existsQuery = $this->connection->createQueryBuilder()
                    ->select($primaryKey)
                    ->from($tableNameSchema)
                    ->where("$primaryKey = ?")
                    ->setParameter(0, $row[$primaryKey])
                    ->executeQuery()
                    ->fetchOne();

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

    private function updateRecord(string $tableNameSchema, array $row, string $primaryKey): void
    {
        $qb = $this->connection->createQueryBuilder();
        $qb->update($tableNameSchema);

        foreach ($row as $column => $value) {
            if ($column !== $primaryKey) {
                $qb->set($column, '?');
                $qb->setParameter(count($qb->getParameters()), $value);
            }
        }

        $qb->where("$primaryKey = ?")
            ->setParameter(count($qb->getParameters()), $row[$primaryKey]);

        $qb->executeStatement();
    }
    private function insertRecord(string $tableNameSchema, array $row, string $primaryKey = 'id'): void
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