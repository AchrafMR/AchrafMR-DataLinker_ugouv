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

class SynchronisationCommand extends Command
{
    protected static $defaultName = 'run:synchronisation';

    private $connection;
    private $httpClient;
    private $entityManager;
    private ContainerInterface $container;

    public function __construct(
        HttpClientInterface $httpClient,
        EntityManagerInterface $entityManager,
        Connection $connection,
        ContainerInterface $container
    ) {
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
                AND TABLE_NAME NOT IN ('synchronisation_info')"; // Exclude these tables

        $stmt = $this->connection->prepare($sql);
        $result = $stmt->executeQuery();
        return $result->fetchAllAssociative();
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
                // $tableName = 'ua_t_commandefrscab';

                // Fetch unsynchronized data from API
                $response = $this->httpClient->request('POST', $ugouvApi . '/api/local/data', [
                    'body' => ['requete' => "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT 10"],
                    'verify_peer' => false,
                    'verify_host' => false,
                ]);

                if ($response->getStatusCode() === 200) {
                    $data = $response->toArray();

                    if (!empty($data)) {
                        // Extract primary key values
                        $columnIds = array_column($data, 'id');
                        $columnIdsString = implode(', ', $columnIds);

                        // Perform upsert operation
                        $this->upsertDataIntoTable($data, $tableName);

                        // Mark the data as synchronized via API
                        $flagResponse = $this->httpClient->request('POST', $ugouvApi . '/api/local/flag', [
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
                    }
                } else {
                    $output->writeln('Failed to fetch data from API for table: ' . $tableName);
                    $this->updateSyncInfo($synchronisation, 'error', $response->getContent(false));
                    return 1; // Failure
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
    
        try {
            // Disable foreign key checks
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' NOCHECK CONSTRAINT ALL');
    
            foreach ($data as $row) {
                $columns = array_keys($row);
                $primaryKey = 'id'; // Assuming 'id' is the primary key
    
                // Check if the record already exists
                $existsQuery = $this->connection->createQueryBuilder()
                    ->select($primaryKey)
                    ->from($tableNameSchema)
                    ->where("$primaryKey = :id")
                    ->setParameter('id', $row[$primaryKey])
                    ->executeQuery()
                    ->fetchOne();
    
                if ($existsQuery) {
                    // Update the record if it exists
                    $qb = $this->connection->createQueryBuilder();
                    $qb->update($tableNameSchema);
    
                    // Add columns and values for the update query
                    foreach ($columns as $column) {
                        if ($column !== $primaryKey) {
                            $qb->set($column, ':' . $column);
                            $qb->setParameter($column, $row[$column]);
                        }
                    }
    
                    $qb->where("$primaryKey = :id")
                       ->setParameter('id', $row[$primaryKey]);
    
                    // Execute the update query
                    $qb->executeStatement();
                } else {
                    // Insert the record if it doesn't exist
                    $qb = $this->connection->createQueryBuilder();
                    $qb->insert($tableNameSchema);

                    // Add columns and values for the insert query
                    foreach ($columns as $column) {
                        $qb->setValue($column, ':' . $column);
                        $qb->setParameter($column, $row[$column]);
                    }

                    // Execute the insert query
                    $qb->executeStatement();
                }
            }

            // Re-enable foreign key constraints
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' WITH CHECK CHECK CONSTRAINT ALL');
            $this->connection->commit();

        } catch (\Exception $e) {
            $this->connection->rollBack();
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' WITH CHECK CHECK CONSTRAINT ALL');
            throw $e; // Re-throw the exception
        }
    }

    private function updateSyncInfo(SynchronisationInfo $synchronisation, string $status, string $message): void
    {
        $synchronisation->setInfo($status);
        $synchronisation->setDateEnd(new \DateTime());
        $synchronisation->setMessage($message);
        $this->entityManager->flush();
    }
}