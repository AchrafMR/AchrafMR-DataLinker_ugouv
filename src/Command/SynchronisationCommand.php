<?php

namespace App\Command;

use Doctrine\DBAL\Connection;
use App\Entity\SynchronisationInfo;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Style\SymfonyStyle;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;


#[AsCommand(
    name: 'run:synchronisation',
    description: 'Add a short description for your command',
)]

class SynchronisationCommand extends Command
{
    protected static $defaultName = 'run:synchronisation';

    private $httpClient;
    private $entityManager;
    // private $ugouvApi;
    private $connection;
    private $databaseName = 'ugouv';
    private ContainerInterface $container;  // Injecting the container


    public function __construct(HttpClientInterface $httpClient, EntityManagerInterface $entityManager, Connection $connection, ContainerInterface $container)
    {
        parent::__construct();
        $this->httpClient = $httpClient;
        $this->entityManager = $entityManager;
        // $this->ugouvApi = $ugouvApi;
        $this->container = $container;  // Storing the container reference
        $this->connection = $connection;
    }

    protected function configure(): void
    {
        $this->setDescription('Synchronizes data between API and local database.')
            ->setHelp('This command fetches unsynchronized data from the API and marks them as synchronized.');
    }
    // Method to query all table names from the database
    private function getAllTableNames(): array
    {
        $sql ="SELECT TABLE_NAME FROM information_schema.tables
              WHERE table_schema = :dbName
              AND TABLE_NAME != 'synchronisation_info'";  // Exclude 'synchronisation_info' table

        $stmt = $this->connection->prepare($sql);
        $stmt->bindValue('dbName', $this->databaseName, \PDO::PARAM_STR);
        $result = $stmt->executeQuery();
        return $result->fetchAllAssociative();
    }


    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        try {
            // Create a new SynchronisationInfo record to log the start of synchronization
            $synchronisation = new SynchronisationInfo();
            $synchronisation->setDateStart(new \DateTime());
            $this->entityManager->persist($synchronisation);
            $this->entityManager->flush();
            $ugouvApi = $this->container->getParameter('ugouv_api');

            
            // Get all table names from the database
            $tables = $this->getAllTableNames();

            // Loop through each table and synchronize the data
            foreach ($tables as $table) {
                $tableName = $table['TABLE_NAME'];

                // Fetch unsynchronized data from the API
                $response = $this->httpClient->request('POST', $ugouvApi . '/api/local/data', [
                    'body' => ['requete' => "SELECT * FROM $tableName WHERE flag_synchronisation = 0 OR flag_synchronisation IS NULL LIMIT 10"],
                    'verify_peer' => false,
                    'verify_host' => false,
                ]);

                if ($response->getStatusCode() === 200) {
                    $data = $response->toArray();

                    if (!empty($data)) {
                        $columnIds = array_column($data, 'id');
                        $columnIdsString = implode(', ', $columnIds);

                        $this->upsertDataIntoTable($data, $tableName);

                        // Mark the data as synchronized via API call
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
                    // Log the error in SynchronisationInfo
                    $output->writeln('Failed to fetch data from API for table: ' . $tableName);
                    $synchronisation->setInfo('error');
                    $synchronisation->setDateEnd(new \DateTime());
                    $synchronisation->setMessage($response->getContent(false));
                    $this->entityManager->flush();
                    return 1; // Return failure
                }
            }

            // If all tables are processed successfully, log success
            $output->writeln('Data synchronized successfully!');
            $synchronisation->setInfo('success');
            $synchronisation->setDateEnd(new \DateTime());
            $synchronisation->setMessage('Data inserted/updated successfully!');
            $this->entityManager->flush();

            return 0; // Return success
        } catch (\Exception $e) {
            // Log any exception in SynchronisationInfo
            $output->writeln('An error occurred: ' . $e->getMessage());
            $synchronisation->setInfo('error');
            $synchronisation->setDateEnd(new \DateTime());
            $synchronisation->setMessage($e->getMessage());
            $this->entityManager->flush();
            return 1;
        }
    }

    // Method to handle inserting or updating data in the local database
    private function upsertDataIntoTable(array $data, string $tableName): void
    {
        $this->connection->beginTransaction();

        try {
            $this->connection->executeQuery('SET foreign_key_checks = 0');

            foreach ($data as $row) {
                $columns = array_keys($row);

                $sql = sprintf(
                    'INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s',
                    $tableName,
                    implode(',', $columns),
                    implode(',', array_map(fn($col) => ':' . $col, $columns)),
                    implode(',', array_map(fn($col) => "$col = VALUES($col)", $columns))
                );

                $stmt = $this->connection->prepare($sql);

                foreach ($row as $column => $value) {
                    $stmt->bindValue(':' . $column, $value);
                }

                $stmt->executeQuery();
            }

            $this->connection->commit();
            $this->connection->executeQuery('SET foreign_key_checks = 1');
        } catch (\Exception $e) {
            $this->connection->rollBack();
            $this->connection->executeQuery('SET foreign_key_checks = 1');
            throw $e;
        }
    }
}