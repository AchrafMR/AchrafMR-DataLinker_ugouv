<?php
namespace App\Command;

use PDO;
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
class InsertExampleCommand extends Command
{
    protected static $defaultName = 'app:insert-example';
    private $connection;
    private $httpClient;
    private $entityManager;
    // private $ugouvApi;
    // private $databaseName = 'ugouv';
    private ContainerInterface $container;

    // Inject the Doctrine DBAL connection
    public function __construct(HttpClientInterface $httpClient, EntityManagerInterface $entityManager, Connection $connection, ContainerInterface $container)
    {
        parent::__construct();
        $this->httpClient = $httpClient;
        $this->entityManager = $entityManager;
        // $this->ugouvApi = $ugouvApi;
        $this->container = $container;  // Storing the container reference
        $this->connection = $connection;
    }
    private function getAllTableNames(): array
    {
        // SQL query to fetch all base table names excluding 'synchronisation_info'
        $sql = "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.tables
                WHERE TABLE_TYPE = 'BASE TABLE'
                AND TABLE_NAME != 'synchronisation_info' AND TABLE_NAME != 'Tbl_Example'";// Exclude 'synchronisation_info'
        $stmt = $this->connection->prepare($sql);
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
            // dd($tables);
            // foreach ($tables as $table) {
            //     // dd($table);
            //     $tableName = $table['TABLE_NAME'];
            //     $schema = $table['TABLE_SCHEMA'];

            //     //  $tableName = 'ua_t_commandefrscab';
            //     // $tableName = 'Tbl_Example';
            //     // dd($tableName) ;

            //     // Fetch unsynchronized data from the API
            //     //LIMIT 10 just for test
            //     $response = $this->httpClient->request('POST', $ugouvApi . '/api/local/data', [
            //         'body' => ['requete' => "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT 10"],
            //         'verify_peer' => false,
            //         'verify_host' => false,
            //     ]);

            //     if ($response->getStatusCode() === 200) {
            //         $data = $response->toArray();
            //         //  dd($data);
            //         if (!empty($data)) {
            //             $columnIds = array_column($data, 'ID');
            //             $columnIdsString = implode(', ', $columnIds);
                        

            //             $this->upsertDataIntoTable($data, $tableName ,$schema);
            //             // Mark the data as synchronized via API call
            //             $flagResponse = $this->httpClient->request('POST', $ugouvApi . '/api/local/flag', [
            //                 'body' => [
            //                     'table' => $tableName,
            //                     'ids' => $columnIdsString,
            //                 ],
            //                 'verify_peer' => false,
            //                 'verify_host' => false,
            //             ]);

            //             if ($flagResponse->getStatusCode() !== 200) {
            //                 throw new \Exception($flagResponse->getContent(false));
            //             }
            //         }
            //     } else {
            //         // Log the error in SynchronisationInfo
            //         $output->writeln('Failed to fetch data from API for table: ' . $tableName);
            //         $synchronisation->setInfo('error');
            //         $synchronisation->setDateEnd(new \DateTime());
            //         $synchronisation->setMessage($response->getContent(false));
            //         $this->entityManager->flush();
            //         return 1; // Return failure
            //     }

            //     // dd('good');
            // }
            foreach ($tables as $table) {
                $tableName = $table['TABLE_NAME'];
                $schema = $table['TABLE_SCHEMA'];
                $tableName = 'ua_t_commandefrscab';

                // Fetch unsynchronized data from the API (this part remains the same)
                $response = $this->httpClient->request('POST', $ugouvApi . '/api/local/data', [
                    'body' => ['requete' => "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT 100"],
                    'verify_peer' => false,
                    'verify_host' => false,
                ]);

                if ($response->getStatusCode() === 200) {
                    $data = $response->toArray();
            
                    if (!empty($data)) {
                        // Extract the primary key values dynamically
                        $columnIds = array_column($data, 'id');
                        $columnIdsString = implode(', ', $columnIds);
            
                        // Continue with upsert and synchronization logic
                        $this->upsertDataIntoTable($data, $tableName, $schema);
            
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
    private function upsertDataIntoTable(array $data, string $tableName, string $schema): void
    {
        $tableNameSchema = "ugouv" . '.' . $tableName;
        $this->connection->beginTransaction();        
        try {

            // Temporarily disable foreign key checks
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' NOCHECK CONSTRAINT ALL');
            // $this->connection->executeQuery('SET IDENTITY_INSERT ' . $tableNameSchema . ' ON');

            foreach ($data as $row) {
                $columns = array_keys($row); // Extract column names
    
                // Use QueryBuilder for inserting data
                $qb = $this->connection->createQueryBuilder();
    
                // Start building the insert query
                $qb->insert($tableNameSchema);
    
                // Add values to the query
                foreach ($columns as $column) {
                    $qb->setValue($column, ':' . $column);
                }
    
                // Bind the parameters to the query
                foreach ($row as $column => $value) {
                    // Doctrine will automatically map the types
                    $qb->setParameter($column, $value);
                }
    
                // Execute the query
                $qb->executeStatement();
            }
            // $this->connection->executeQuery('SET IDENTITY_INSERT ' . $tableNameSchema . ' OFF');

            // Re-enable foreign key constraints
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' WITH CHECK CHECK CONSTRAINT ALL');
            // Commit the transaction
            $this->connection->commit();
    
        } catch (\Exception $e) {
            // Rollback transaction on failure
            $this->connection->rollBack();
    
            // Re-enable foreign key checks in case of failure
            $this->connection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' WITH CHECK CHECK CONSTRAINT ALL');
            // $this->connection->executeQuery('SET IDENTITY_INSERT ' . $tableNameSchema . ' OFF');
    
            throw $e; // Re-throw the exception
        }
    }
    
    
    // public function getTypePDO($type) {
    //     $typeMap = [
    //         'int' => PDO::PARAM_INT,
    //         'bigint' => PDO::PARAM_INT,
    //         'smallint' => PDO::PARAM_INT,
    //         'tinyint' => PDO::PARAM_INT,
    //         'decimal' => PDO::PARAM_STR,
    //         'numeric' => PDO::PARAM_STR,
    //         'money' => PDO::PARAM_STR,
    //         'smallmoney' => PDO::PARAM_STR,
    //         'float' => PDO::PARAM_STR,
    //         'real' => PDO::PARAM_STR,
    //         'varchar' => PDO::PARAM_STR,
    //         'nvarchar' => PDO::PARAM_STR,
    //         'char' => PDO::PARAM_STR,
    //         'nchar' => PDO::PARAM_STR,
    //         'text' => PDO::PARAM_STR,
    //         'ntext' => PDO::PARAM_STR,
    //         'xml' => PDO::PARAM_STR,
    //         'date' => PDO::PARAM_STR,
    //         'datetime' => PDO::PARAM_STR,
    //         'datetime2' => PDO::PARAM_STR,
    //         'smalldatetime' => PDO::PARAM_STR,
    //         'time' => PDO::PARAM_STR,
    //         'timestamp' => PDO::PARAM_STR,
    //         'bit' => PDO::PARAM_BOOL,
    //         'binary' => PDO::PARAM_LOB,
    //         'varbinary' => PDO::PARAM_LOB,
    //         'image' => PDO::PARAM_LOB,
    //         'uniqueidentifier' => PDO::PARAM_STR,
    //         'json' => PDO::PARAM_STR,
    //     ];
    
    //     // Default to string type if the type is not recognized
    //     return $typeMap[$type] ?? PDO::PARAM_STR;
    // }

}