<?php
/*
   +--------------------------------------------------------------------+
   | Copyright Rich Lott 2021. All rights reserved.                     |
   |                                                                    |
   | This work is published under the GNU AGPLv3 license with some      |
   | permitted exceptions and without any warranty. For full license    |
   | and copyright information, see LICENSE                             |
   +--------------------------------------------------------------------+
 */
namespace App\Command;

use League\Csv\Reader;
use League\Csv\Stream;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Helper\ProgressBar;
use Symfony\Component\Filesystem\Filesystem;


class ConvertCommand extends Command
{
    public static $maxint = 0;

    protected $outputFile;
    protected $filesystem;
    protected function configure() {
        $this->setName('convert')
            ->setDescription('Convert CSV file to MySQL SQL')
            ->setHelp('')
            // ->addOption('output', 'o', InputOption::VALUE_REQUIRED, 'output SQL filename')
            ->addOption('drop', 'd', InputOption::VALUE_NONE, 'Include DROP TABLE? (default no, and use CREATE TABLE IF NOT EXISTS)')
            ->addOption('schema', 's', InputOption::VALUE_NONE, 'Just the schema, no INSERTs')
            ->addOption('import-extension', 'e', InputOption::VALUE_REQUIRED, 'Extension of files to import (i.e. csv or dat)','dat')
            ->addOption('csv-read-buffer', 'b', InputOption::VALUE_REQUIRED, 'Buffer size in kB. Each line of CSV must be shorter than this. Default 10', 10)
            ->addOption('max-command-length', 'm', InputOption::VALUE_REQUIRED, 'Max length of INSERT SQL command in kB. Default 10.', 10)
            ->addArgument('inputfiles', InputArgument::IS_ARRAY | InputArgument::REQUIRED, "CSV files or Directory of files to convert")
            ;
    }

    protected function execute(InputInterface $input, OutputInterface $output) {

        $inputfiles = $input->getArgument('inputfiles');

        $extension = $input->getOption('import-extension');

        $files = array();
        $this->filesystem = new Filesystem();
        $this->outputFile = "output.sql";
        //$this->filesystem->remove($this->outputFile);

        foreach ($inputfiles as $inputfile) {
            // Open a known directory, and proceed to read its contents
            if (is_dir($inputfile)) {
                $dirfiles = glob($inputfile . "/*." . $extension);
                if ($dirfiles) {
                    $files += $dirfiles;
                }
            } else {
                $files[] = $inputfile;
            }
        }

        foreach ($files as $file) {
            $this->executeOne($input, $output, $file);
        }
        return 0;
    }

    protected function executeOne(InputInterface $input, OutputInterface $output, $csvFilename) {


        if (!file_exists($csvFilename)) {
            $output->writeln("<error>Error: input '$csvFilename' not found</error>");
            return 1;
        }
        if (!is_readable($csvFilename)) {
            $output->writeln("<error>-- Error: input '$csvFilename' not readable</error>");
            return 1;
        }
        try {
            $reader = Reader::createFromPath($csvFilename, 'r');
            $reader->setDelimiter('|');
        }
        catch (\Exception $e) {
            $output->writeln("<error>-- Error: input '$csvFilename': " . $e->getMessage() . "</error>");
            return 1;
        }

        //$isBigFile = ($reader->count() > 100);


        $path_parts = pathinfo($csvFilename);

        $tableName = preg_replace( '/[^a-zA-Z0-9_]+/', '_', $path_parts['filename']);

        // Determine column types.
        $schemaCols = $indexCols = $columns = $column_keys = [];

        $foundID = FALSE;

        $skipHeaders = [
            '^',
            '',
        ];

        $ignoreDuplicateHeader = true;

        $type_tests = [
            'unsigned_int' => '/^\d+$/', 
            'signed_int' => '/^-?\d+$/', 
            'float' => '/^\d+\.?\d*$/', 
            'datetime' => '/^\d\d\d\d-\d\d-\d\d[T ]\d\d:\d\d:\d\d\.?\d*$/',
            'date' => '/^\d\d\d\d-\d\d-\d\d$/', 
        ];

        $progressBar = new ProgressBar($output);
        $progressBar->setMessage("Scanning schema.");

        $nr_cols = 0;

        foreach ($progressBar->iterate($reader) as $offset => $row) {
            if ($offset == 0) {
                //Skip Column Names.
                foreach ($row as $index => $colname) {
                    $nr_cols++;
                    if (in_array($colname, $skipHeaders)) continue;

                    //Ignore duplicates.
                    if ($ignoreDuplicateHeader and in_array($colname, $columns)) continue;


                    $columns[$index] = [
                        'name' => trim(preg_replace('/[^a-zA-Z0-9]+/', '_', $colname)),
                        'tests' => [
                            'unsigned_int', 
                        'signed_int', 
                        'float', 
                        'datetime',
                        'date', 
                        ],
                        'type' => 'text',
                        'maxlength' => 0,
                        'maxmblength' => 0,
                        'maxint' => 0,
                        'empty' => TRUE,
                    ];
                }
                $column_keys = array_keys($columns);
                continue;
            }

            if (count($row) != $nr_cols) continue;

            //Figure out type for each column.
            foreach ($columns as $index => &$column) {
                //Skip if empty
                $value = $row[$index];
                if (is_null($value) or $value == '') continue; 

                $tests = $column['tests'];

                $column['empty'] = FALSE;

                foreach ($tests as $test_type) {
                    if (preg_match($type_tests[$test_type], $value) == false) {
                        //If no match.  Then the column is not this type.
                        $column['tests'] = array_diff($column['tests'], [$test_type]);
                    } else {
                        //break;
                    }
                }
                if (in_array($test_type, ['signed_int', 'unsigned_int'])) {
                    $column['maxint'] = $column['maxint'] < $value ? $value : $column['maxint']; 
                }
                $maxlength = strlen($value);
                $column['maxlength'] = $maxlength > $column['maxlength'] ? $maxlength : $column['maxlength'];
                $maxmblength = mb_strlen($value);
                $column['maxmblength'] = $maxmblength > $column['maxmblength'] ? $maxmblength : $column['maxmblength'];
            }
        }

        foreach ($columns as $index => $t) {

            if (count($t['tests'])) {
                $type = array_shift($t['tests']);
            } else {
                $type = 'text';
            }

            if (preg_match("/_id$/", $t['name'])) {
                $indexCols[] = $t['name'];
            }

            // Great.
            if ($type === 'text') {
                if ($t['maxlength'] > 65535) {
                    $t['def'] = 'MEDIUMTEXT';
                }
                else if ($t['maxlength'] > 255) {
                    $t['def'] = 'TEXT';
                }
                else {
                    // allow 10% more than the max chars so far.
                    $t['def'] = 'VARCHAR(' . ((int) (1.10 * $t['maxmblength'])) . ')';
                }
                // We don't know that we need to differentiate between zero length string and NULL;
                // the INSERTS will be zls, so call this column NOT NULL.
                $t['def'] .= ' NOT NULL';
            }
            elseif ($type === 'unsigned_int') {
            // @see https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
                if ($t['maxint'] <= 255) {
                    $t['def'] = 'TINYINT UNSIGNED';
                }
                else if ($t['maxint'] <= 4294967295) {
                    $t['def'] = 'INT(10) UNSIGNED';
                }
                else {
                    $t['def'] = 'BIGINT UNSIGNED';
                }

                if (!$t['empty']) {
                    $t['def'] .= ' NOT NULL DEFAULT 0';
                }
            }
            elseif ($type === 'signed_int') {
                // @see https://dev.mysql.com/doc/refman/8.0/en/integer-types.html
                if ($t['maxint'] >= -128 && $t['maxint'] <= 127) {
                    $t['def'] = 'TINYINT SIGNED';
                }
                elseif ($t['maxint'] >= -2147483648 && $t['maxint'] <= 2147483647) {
                    $t['def'] = 'INT(10) SIGNED';
                }
                else {
                    $t['def'] = 'BIGINT SIGNED';
                }

                if (!$t['empty']) {
                    $t['def'] .= ' NOT NULL DEFAULT 0';
                }
            }
            elseif ($type === 'float') {
                $t['def'] = 'DECIMAL(12,4)';
                if (!$t['empty']) {
                    $t['def'] .= ' NOT NULL DEFAULT 0';
                }
            }
            elseif ($type === 'date') {
                $t['def'] = 'DATE';
                // All date columns are created with NULL as default
                // since setting a default requires a specific date.
                // if (!$t['empty']) {
                //   $t['def'] .= ' NOT NULL DEFAULT 0';
                // }
            }
            elseif ($type === 'datetime') {
                $t['def'] = 'TIMESTAMP';
                if (!$t['empty']) {
                    $t['def'] .= ' NOT NULL DEFAULT CURRENT_TIMESTAMP';
                }
            }
            else {
                throw new \RuntimeException("Row " . $offset . " col '" . $colname . "' unexpected type '$type'");
            }

            $schemaCols[$t['name']] = $t['def'];
            if (strtolower($t['name']) === 'id') {
                $schemaCols[$t['name']] .= ' PRIMARY KEY';
                $foundID = $t['name'];
            }
        }

        $schemaSQL = "\n\n";
        if ($input->getOption('drop')) {
            $schemaSQL = "DROP TABLE IF EXISTS `$tableName`;\n";
        }
        $schemaSQL .= "CREATE TABLE IF NOT EXISTS `$tableName` (\n";
        // Add an ID if there is none.
        //$prefix = '';
        $prefix = "  ";
        if (!$foundID) {
            //$schemaSQL .= "  id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY";
            //$prefix = ",\n  ";
        }

        foreach ($schemaCols as $colName => $def) {
            $schemaSQL .= $prefix . "`$colName` $def";
            $prefix = ",\n  ";
        }
        $schemaSQL .= "\n);\n";

        $indexSQL = "ALTER TABLE `$tableName`\n";

        $index_lines[] = array();

        foreach ($indexCols as $colName) {
            if ($tableName . "_id" == $colName) {
                $index_line[] = "\tADD PRIMARY KEY `$colName` (`$colName`)";
            } else {
                $index_line[] = "\tADD KEY `$colName` (`$colName`)";
            }
        }

        $indexSQL .= implode(",\n", $index_line) . ";\n";


        $output->writeln("<info>$schemaSQL</info>");
        $output->writeln("<info>$indexSQL</info>");
        $this->filesystem->appendToFile($this->outputFile, $schemaSQL);
        $this->filesystem->appendToFile($this->outputFile, $indexSQL);

        if (isset($progressBar)) {
            $progressBar->finish();
            $progressBar->clear();
        }

        if ($input->getOption('schema')) {
            return 0;
        }

        // Output the data.
        $insert = "INSERT INTO `$tableName` (`"
            . implode('`, `', array_keys($schemaCols))
            . "`) VALUES \n";

        // 10k per SQL command.
        $maxCommandLength = 1024*$input->getOption('max-command-length');
        $maxValuesLength = $maxCommandLength - strlen($insert);
        $command = $insert;
        $sep = '';
        if (isset($progressBar)) {
            $progressBar = new ProgressBar($output, count($reader));
            $progressBar->setFormat('custom');
            $progressBar->setMessage("Writing INSERTS");
            $progressBar->start();
        }
        foreach ($records as $row) {
            $data = [];
            foreach ($cols as $header=>$col) {
                $val = $row->$header;
                if (in_array($col['type'], ['unsigned_int', 'signed_int', 'float'])) {
                    // We can trust this value to be safe because of the regex above.
                    // Cast explicit empty number values to NULL.
                    if ($val === '') {
                        $val = 'NULL';
                    }
                }
                else {
                    // Text for everything else.
                    $val = '"' . str_replace('"', '\\"', $val) . '"';
                }
                $data[] = $val;
            }
            $data = '(' . implode(',', $data) . ')';
            if (strlen($data) > $maxValuesLength) {
                throw new \RuntimeException("Row " . $row->key() . " exceeds max SQL command length");
            }
            if (strlen($data) + strlen($command) > $maxCommandLength) {
                // complete last command.
                $command .= ";\n";
                $sep = '';
                $output->writeln($command);
                $command = $insert;
            }
            $command .= $sep . $data;
            $sep = ',';

            if (isset($progressBar)) {
                $progressBar->advance();
            }
        }
        $command .= ";\n";
        $output->writeln($command);
        if (isset($progressBar)) {
            $progressBar->finish();
            $progressBar->clear();
        }

        return 0;
    }
}
