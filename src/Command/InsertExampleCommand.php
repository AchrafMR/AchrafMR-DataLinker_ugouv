<?php
namespace App\Command;

use Doctrine\DBAL\Connection;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

class InsertExampleCommand extends Command
{
    protected static $defaultName = 'app:insert-example';
    private $connection;

    // Inject the Doctrine DBAL connection
    public function __construct(Connection $connection)
    {
        parent::__construct();
        $this->connection = $connection;
    }

    protected function configure(): void
    {
        // Set a description for the command
        $this->setDescription('Inserts a new record into Tbl_Example using a raw SQL query in SQL Server');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);

        // Define the SQL query
        $sql = "INSERT INTO Tbl_Example (ID, Name, flag_synchronisation_locale) VALUES (:id, :name, :flag)";

        try {
            // Prepare the statement
            $statement = $this->connection->prepare($sql);

            // Execute the query with the parameter values
            $statement->executeStatement([
                'id' => 2,
                'name' => 'Jane Smith',
                'flag' => 0, // Use the correct flag value
            ]);

            $io->success('Inserted a new record into Tbl_Example using raw SQL in SQL Server.');
        } catch (\Exception $e) {
            $io->error('Error: ' . $e->getMessage());
            return Command::FAILURE;
        }

        return Command::SUCCESS;
    }
}
