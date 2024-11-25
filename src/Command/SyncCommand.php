<?php


namespace App\Command;

use Doctrine\DBAL\Connection;
use App\Entity\SynchronisationInfo;
use Doctrine\ORM\EntityManagerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Bundle\FrameworkBundle\Console\Application;

class SyncCommand extends Command
{
    protected static $defaultName = 'run:sync';
    private EntityManagerInterface $entityManager;

    public function __construct(EntityManagerInterface $entityManager)
    {
        parent::__construct();
        $this->entityManager = $entityManager; // Initialize the entity manager
    }

    protected function configure(): void
    {
        $this->setDescription('Synchronizes data from MySQL to SQL Server.');
    }
    private function initializeSynchronization(): SynchronisationInfo
    {
        $synchronisation = new SynchronisationInfo();
        $synchronisation->setDateStart(new \DateTime());
        $this->entityManager->persist($synchronisation);
        $this->entityManager->flush();
        return $synchronisation;
    }
    private function initializeConnections(): void
    {
        $doctrine = $this->getApplication()->getKernel()->getContainer()->get('doctrine');
        $defaultEntityManager = $doctrine->getManager();
        $ugouvEntityManager = $doctrine->getManager('ugouv');

        $this->sqlServerConnection = $defaultEntityManager->getConnection();
        $this->mysqlConnection = $ugouvEntityManager->getConnection();
    }
    private function extractPrimaryKeyValues(array $data, array $primaryKey): array
    {
        $columnIds = [];

        foreach ($data as $row) {
            $keyValues = [];

            foreach ($primaryKey as $keyColumn) {
                // Determine if it's a composite key (array format) or single key (string)
                $columnName = is_array($keyColumn) && isset($keyColumn['ColumnName']) ? $keyColumn['ColumnName'] : $keyColumn;
                $keyValues[] = $row[$columnName] ?? null;
            }

            // Join composite key values with a dash; for single keys, it just contains one value
            $columnIds[] = implode('-', $keyValues);
        }

        return $columnIds;
    }

    /**
     * Handles synchronization errors by logging and updating the SynchronisationInfo record.
     */
    private function handleSynchronizationError(SynchronisationInfo $synchronisation, string $tableName, \Exception $e): void
    {
        $status = 'error in Table ' . ($tableName ?: 'N/A');
        $message = $e->getMessage() . ' in file ' . $e->getFile() . ' on line ' . $e->getLine();

        // Use the existing updateSyncInfo method
        $this->updateSyncInfo($synchronisation, $status, $message);
    }
    private function getAllTableNames(): array
    {
//        $sql = "SELECT TABLE_NAME
//                FROM information_schema.tables
//                WHERE TABLE_TYPE = 'BASE TABLE'
//                  AND TABLE_SCHEMA = 'ugouv'
//                  AND TABLE_NAME IN (
//                      't_achatdemandeinternecab',
//                      'ua_t_commandefrscab',
//                      'ua_t_livraisonfrscab',
//                      'ua_t_facturefrscab',
//                      'uv_deviscab',
//                      'uv_commandecab',
//                      'uv_livraisoncab',
//                      'uv_facturecab',
//                      'p_dossier',
//                      'u_p_partenaire',
//                      'p_partenaire_categorie',
//                      'u_general_operation',
//                      'tr_transaction'
//                  );";

        $sql = "SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA = 'ugouv'
        AND TABLE_NAME NOT IN (
            'user_created_id',
            '_biomed_14_09_22_(2)',
            '_biomed',
            '_biomed_14_09_22',
            '_biomed_15_09_22_mod',
            'umouvement_antenne_',
            'matrice_plan_comptable',
            'univ_p_mesure_alerte',
            'utype',
            'univ_t_admission_documents',
            'ua_t_livraisonfrsdet_quantite',
            'univ_p_direction',
            'univ_xseance',
            'univ_t_preinscription',
            'ucategory',
            'univ_t_alerte',
            'univ_t_etudiant_appel',
            'uv_chargedevis',
            'u_p_affaire',
            'notification',
            'pprojet_sous',
            'univ_ex_controle_module',
            'tachatdemandeinternecab_fichier',
            'univ_pl_emptimens',
            'univ_p_document',
            'univ_ac_epreuve',
            'univ_d_prediplomes',
            'p_forme',
            'arc_notification',
            'p_article_nature',
            'u_p_commandety',
            'parametrage',
            'univ_p_nature_alerte',
            'grs_employe',
            'ua_t_reglementfrs',
            'univ_xseance_absences',
            'taches_users',
            'arc_projet',
            'univ_mouchard',
            'udepot',
            'p_forme_juridique',
            'demand_status',
            'tr_charge',
            'univ_t_etudiant_bloque',
            'univ_facetemp',
            'p_article_niveau',
            'demand_stock_cab',
            'univ_pl_emptimens_type',
            'u_p_devise',
            'univ_t_brdpaiement',
            'univ_d_service',
            'p_global_param',
            'uv_commandecab',
            'univ_ex_controle_promotion',
            'univ_p_document_attribution',
            'psituation_familiale',
            'us_module_parametrage',
            'univ_p_organisme',
            'p_grade',
            'us_modules_dossiers',
            'parametrage_output',
            'univ_xseance_autorisation',
            'u_p_partenaire_msj',
            'us_operation',
            'univ_d_user_service',
            'univ_nature_demande',
            'ptaille',
            'u_p_partenaire',
            'univ_xseance_autorisation_lg',
            'univ_t_preinscription_cab',
            'univ_p_document_bourse',
            't_achatdemandeinternedet',
            'univ_h_albhon',
            'uv_ta_inter',
            'p_marches_dossiers',
            'p_article_niveau_old',
            'ptype_conge',
            'univ_pr_concours',
            'particle_niveau',
            'univ_xseance_capitaliser',
            'univ_p_salle',
            'univ_division_groupe',
            'p_compte_banque_type',
            'p_modepaiement',
            'univ_xseance_justif',
            'univ_ac_etablissement',
            'univ_t_grpins',
            'univ_ex_controle_semestre',
            'ufacture_type',
            'p_compte',
            'uv_ta_inter_old',
            'p_comptemasse',
            'univ_num_run_deil',
            'ptype_contrat',
            'uarticle_fichier',
            'p_nomenclature_standard',
            'ua_t_facturefrsdet',
            'ufamille',
            'univ_t_conditionpaiement',
            'univ_t_preinscription_documents',
            'ua_t_commandefrscab_acompte',
            'univ_xseance_motif_abs',
            'arc_tree',
            't_conditionpaiement',
            'umouvement_antenne',
            'univ_t_preinscription_documents_bource',
            'tr_chargedet',
            'v_chargedevis',
            'pc_fcz',
            'univ_xseance_sanction',
            'univ_p_signataire',
            'univ_division_groupe_detail',
            'univ_p_enseignant',
            'puser',
            'univ_t_preinscription_releve_note',
            'univ_p_anonymat_actuel',
            'p_partenaire',
            'p_compteposte',
            'ta_client',
            'uarticle_prix',
            'pcompte_banque_pdossier',
            'demande_stock_det',
            'univ_t_inscription',
            'p_compte_banque',
            'univ_ex_enotes',
            'pcondition_reglement',
            'univ_p_situation',
            'uv_facturedet',
            'univ_t_convocation',
            'univ_h_honens',
            'ua_technique_cab',
            'univ_ac_formation',
            'ua_t_livraisonfrscab',
            'univ_p_statut',
            'univ_p_batiment',
            'pcounter',
            'type_partenaire',
            'univ_t_preinscription_suivi',
            'demande_type_op',
            'tr_charges_reglements',
            'ua_technique_det',
            'us_parametrage',
            'univ_pr_concoursdet',
            'u_articles_categories',
            'univ_ep_concours',
            'tr_commandecab',
            'arc_tree_cab',
            'umouvement_antenne_old',
            'devis_technique_cab',
            'p_compterubrique',
            'uarticleoldd',
            'u_general_operation',
            'univ_p_enseignant_except',
            'ua_t_commandefrsdet',
            'univ_p_charge_facture',
            'ta_commentaire',
            'umouvement_stock',
            'univ_etudiant_groupe',
            'devis_technique_det',
            'univ_ac_module',
            'univ_t_etudiant',
            'arc_tree_det',
            'uv_livraisoncab',
            'us_solution',
            'pdepartement',
            'univ_p_concours_grille',
            'univ_pr_correspondance',
            'p_piece',
            'univ_t_reglement',
            'univ_xseance_sanction_lg',
            'ua_tet_dec',
            'p_d',
            'u_p_partenaire_ty',
            'univ_i_seance',
            'univ_p_enseignant_grille',
            'univ_t_inscription_imp_controle',
            'ua_t_facturefrscab',
            'grs_grille_conge',
            'univ_xseance_service',
            'ta_commentaire_file',
            'univ_xseance_stage',
            'univ_p_statutepreuve',
            'article_old',
            'univ_ex_anotes',
            'u_p_projet',
            'univ_xseance_stage_planing',
            'tr_commandedet',
            'ecriture_cab',
            'pdocument',
            'univ_ac_promotion',
            'pville',
            'ua_tet_enc',
            'univ_p_concours_matieres',
            'ta_priorite',
            'us_sous_module',
            'univ_p_type_element',
            'univ_pr_nature_epreuve',
            'u_p_partenaire_10042023',
            'up_piece',
            'univ_p_concourscab',
            'p_poste',
            'rh_paie',
            'pdossier_organisation',
            'univ_t_inscription_imp_log',
            'umouvement_stock_encours',
            'univ_p_estatut',
            'ta_projet',
            's_livraisonfrsdet',
            'grs_note_interne',
            'univ_p_ville',
            'tr_operations_transactions',
            'uv_deviscab_fichier',
            'sheet1',
            'univ_ex_fnotes',
            'article_plan_comptable',
            'upresponsable',
            'uantenne',
            'univ_tpreinscription_fichier',
            'tr_transaction'
        )";

//        $sql = "SELECT TABLE_NAME FROM information_schema.tables
//                WHERE TABLE_TYPE = 'BASE TABLE'
//                AND TABLE_SCHEMA = 'ugouv'
//                AND TABLE_NAME NOT IN ('user_created_id','_biomed_14_09_22_(2)','_biomed','_biomed_14_09_22','_biomed_15_09_22_mod','umouvement_antenne_')";

        $stmt = $this->sqlServerConnection->prepare($sql);
        return $stmt->executeQuery()->fetchAllAssociative();
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        // Set unlimited execution time and memory limit
        set_time_limit(0);
        ini_set('memory_limit', '-1');

        $tableName = '';
        try {
            $synchronisation = $this->initializeSynchronization();
            $output->writeln('Starting data synchronization...');
            $this->initializeConnections();

            $tables = $this->getAllTableNames();
//          dd($tables);
            $tableCount = 1;

            foreach ($tables as $table) {
                $tableName = $table['TABLE_NAME'];
                $tableName='fac_hosix';

                $output->writeln("$tableCount Processing table: $tableName");
                $tableCount++;
                $moreData = true;
                $limit = 1;

                while ($moreData) {

                    $data = $this->fetchUnsynchronizedData($tableName, $limit);
                    if (!empty($data)) {
                        $primaryKey = $this->getIdOrPrimaryKey($tableName);

                        if (!$primaryKey) {
                            throw new \Exception("Table $tableName does not contain an 'id' column or primary key.");
                        }

                        $this->extractPrimaryKeyValues($data, $primaryKey);
//                        dd($columnIds);
                        $this->upsertDataIntoTable($data, $tableName, $primaryKey);
                        $this->flagDataAsSynchronized($tableName, $primaryKey, $data);
                    } else {
                        $moreData = false;
                    }
                }
                gc_collect_cycles();
            }

            $output->writeln('Data synchronized successfully!');
            $this->updateSyncInfo($synchronisation, 'success', 'Synchronization completed successfully.');
            return Command::SUCCESS;

        } catch (\Exception $e) {
            $output->writeln('Error with table ' . ($tableName ?: 'N/A') . ': ' . $e->getMessage());
            $this->handleSynchronizationError($synchronisation, $tableName, $e);
            return Command::FAILURE;
        }
    }


    private function fetchUnsynchronizedData(string $tableName, int $limit): array
    {
        $sql = "SELECT * FROM $tableName WHERE flag_synchronisation_locale = 0 OR flag_synchronisation_locale IS NULL LIMIT $limit";
        $stmt = $this->mysqlConnection->prepare($sql);
        return $stmt->executeQuery()->fetchAllAssociative();
    }

    private function flagDataAsSynchronized(string $tableName, array $primaryKey, array $data): void
    {
        if (empty($data)) {
            return;
        }

        $whereClauses = [];
        foreach ($data as $row) {
            $conditions = [];
            if (isset($primaryKey[0]['ColumnName'])) {
                foreach ($primaryKey as $keyColumn) {
                    $columnName = $keyColumn['ColumnName'];
                    $conditions[] = "$columnName = " . $this->mysqlConnection->quote($row[$columnName]);
                }
            } else {
                $primaryKeyColumn = $primaryKey[0];
                $conditions[] = "$primaryKeyColumn = " . $this->mysqlConnection->quote($row[$primaryKeyColumn]);
            }
            $whereClauses[] = '(' . implode(' AND ', $conditions) . ')';
        }

        $whereClause = implode(' OR ', $whereClauses);

        $updateSql = "UPDATE $tableName SET flag_synchronisation_locale = 1 WHERE $whereClause";
        $this->mysqlConnection->executeQuery($updateSql);
    }

    private function upsertDataIntoTable(array $data, string $tableName, array $primaryKey): void
    {
        $tableNameSchema = "ugouv" . '.' . $tableName;
        $this->sqlServerConnection->beginTransaction();

        try {
            $this->sqlServerConnection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' NOCHECK CONSTRAINT ALL');
            $columnTypes = $this->getColumnTypes($tableName, 'ugouv');

            foreach ($data as $row) {
                foreach ($row as $column => $value) {
                    if (in_array($columnTypes[$column], ['datetime', 'date', 'datetime2'])) {
                        if (!$this->validateDate($value)) {
                            $row[$column] = null;
                        }
                    }
                }

                $whereConditions = [];
                if (isset($primaryKey[0]['ColumnName'])) {
                    foreach ($primaryKey as $keyColumn) {
                        $columnName = $keyColumn['ColumnName'];
                        $whereConditions[] = "$columnName = " . $this->sqlServerConnection->quote($row[$columnName]);
                    }
                } else {
                    $primaryKeyColumn = $primaryKey[0];
                    $whereConditions[] = "$primaryKeyColumn = " . $this->sqlServerConnection->quote($row[$primaryKeyColumn]);
//                    dd($whereConditions)
                }

                $whereClause = implode(' AND ', $whereConditions);
                $existsQuery = $this->sqlServerConnection->createQueryBuilder()
                    ->select('*')
                    ->from($tableNameSchema)
                    ->where($whereClause)
                    ->executeQuery()
                    ->fetchOne();

                if ($existsQuery) {
                    $this->updateRecord($tableNameSchema, $row, $primaryKey);
                } else {
                    $this->insertRecord($tableNameSchema, $row);
                }
            }

            $this->sqlServerConnection->executeQuery('ALTER TABLE ' . $tableNameSchema . ' CHECK CONSTRAINT ALL');
            $this->sqlServerConnection->commit();

        } catch (\Exception $e) {
            $this->sqlServerConnection->rollBack();
            throw $e;
        }
    }

    private function getIdOrPrimaryKey(string $tableName): ?array
    {
        $idCondition = "(COLUMN_NAME = 'id' OR COLUMN_NAME = 'ID')";
        $sql = "SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '$tableName'
            AND $idCondition
            AND TABLE_SCHEMA = 'ugouv';";

        $stmt = $this->sqlServerConnection->prepare($sql);
        $result = $stmt->executeQuery();
        $idColumn = $result->fetchOne();

        if ($idColumn) {
            return [$idColumn];
        }

        $primaryKeys = $this->getPrimaryKeys($tableName);
        return count($primaryKeys) > 0 ? $primaryKeys : null;
    }

    private function getPrimaryKeys(string $tableName): array
    {
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
            AND t.name = '$tableName'";

        $stmt = $this->sqlServerConnection->prepare($sql);
        $result = $stmt->executeQuery();
        return $result->fetchAllAssociative();
    }

    private function validateDate($date, $format = 'Y-m-d H:i:s'): bool
    {
        // Return false for null, empty, or invalid placeholder dates starting with 0000
        if ($date === null || $date === '' || preg_match('/^0000/', $date)) {
            return false;
        }

        // Try to parse the date with the provided format
        $d = \DateTime::createFromFormat($format, $date);


        // Check if parsing succeeded and matches the format
        if ($d && $d->format($format) === $date) {
            return true;
        }

        // If parsing with the provided format fails, check for 'Y-m-d' (date-only) format
        $d = \DateTime::createFromFormat('Y-m-d', $date);
        return $d && $d->format('Y-m-d') === $date;
    }

//    private function validateDate($date, $format = 'Y-m-d H:i:s'): bool
//    {
//        if ($date === null || $date === '' || $date === '0000-00-00' || $date === '0000-00-00 00:00:00') {
//            return false;
//        }
//
//        $d = \DateTime::createFromFormat($format, $date);
//        return $d && $d->format($format) === $date;
//    }
//
    private function getColumnTypes(string $tableName, string $schema): array
    {
        $sql = "
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = ?
            AND TABLE_SCHEMA = ?";

        $stmt = $this->sqlServerConnection->prepare($sql);
        $stmt->bindValue(1, $tableName);
        $stmt->bindValue(2, $schema);

        return $stmt->executeQuery()->fetchAllKeyValue();
    }

    private function updateRecord(string $tableNameSchema, array $row, array $primaryKey): void
    {
        $qb = $this->sqlServerConnection->createQueryBuilder();
        $qb->update($tableNameSchema);

        foreach ($row as $column => $value) {
            if (!in_array($column, array_column($primaryKey, 'ColumnName')) && !in_array($column, $primaryKey)) {
                $qb->set($column, '?');
                $qb->setParameter(count($qb->getParameters()), $value);
            }
        }

        if (isset($primaryKey[0]['ColumnName'])) {
            $conditions = [];
            foreach ($primaryKey as $keyColumn) {
                $columnName = $keyColumn['ColumnName'];
                $conditions[] = "$columnName = ?";
                $qb->setParameter(count($qb->getParameters()), $row[$columnName]);
            }
            $qb->where(implode(' AND ', $conditions));
        } else {
            $primaryKeyColumn = $primaryKey[0];
            $qb->where("$primaryKeyColumn = ?");
            $qb->setParameter(count($qb->getParameters()), $row[$primaryKeyColumn]);
        }

        $qb->executeStatement();
    }

    private function insertRecord(string $tableNameSchema, array $row): void
    {
        $qb = $this->sqlServerConnection->createQueryBuilder();
        $qb->insert($tableNameSchema);

        foreach ($row as $column => $value) {
            if ($column != 'user' && $column != 'public') {
                $qb->setValue($column, '?');
            } else {
                $qb->setValue('[' . $column . ']', '?');
            }
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
