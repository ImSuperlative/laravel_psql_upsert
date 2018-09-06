<?php

namespace imsuperlative\Upsert;

use Illuminate\Database\Grammar;

trait UpsertTrait
{

    /**
     * @param array      $values
     * @param array|null $updateColumns
     *
     * @return bool
     */
    public static function upsert(array $values, array $updateColumns = null)
    {
        if (empty($values)) {
            return true;
        }

        if (! is_array(reset($values))) {
            $values = [$values];
        } else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        $model = self::getModel();
        $sql = static::compileUpsert($model->getConnection()->getQueryGrammar(), $values, $updateColumns);
        $values = static::inLineArray($values);

        return $model->getConnection()->affectingStatement($sql, $values);
    }

    /**
     * @return mixed
     */
    public static function getModel()
    {
        $model = get_called_class();

        return new $model;
    }

    /**
     * @param \Illuminate\Database\Grammar $grammar
     * @param array                        $values
     * @param array|null                   $updateColumns
     *
     * @return string
     */
    private static function compileUpsert(Grammar $grammar, array $values, array $unique = null)
    {
        $table = static::getTableName();
        $columns = $grammar->columnize(array_keys(reset($values)));
        $parameters = collect($values)->map(function ($record) use ($grammar) {
            return '(' . $grammar->parameterize($record) . ')';
        })->implode(', ');

        $insert = "INSERT INTO $table ($columns) VALUES $parameters";

        if (empty($unique)) {
            $unique[] = static::getTablePrimaryKey();
        }

        $keys = array_keys(reset($values));

        // excluded fields are all fields except $unique one that will be updated
        // also created_at should be excluded since record already exists
        $excluded = array_filter($keys, function ($e) use ($unique) {
            return !in_array($e, $unique) && $e != static::getTableCreatedAt();
        });

        $update = join(', ', array_map(function ($e) {
            return "\"$e\" = \"excluded\".\"$e\"";
        }, $excluded));

        $unique = implode(', ', $unique);

        return "$insert on conflict ($unique) do update set $update";
    }

    /**
     * @return string
     */
    private static function getTableName()
    {
        $model = self::getModel();

        return $model->getConnection()->getTablePrefix() . $model->getTable();
    }

    /**
     * @return string
     */
    private static function getTablePrimaryKey()
    {
        $model = self::getModel();

        return $model->getKeyName();
    }

    /**
     * @return string
     */
    private static function getTableCreatedAt()
    {
        $model = self::getModel();

        return $model->getCreatedAtColumn();
    }

    /**
     * @param array $records
     *
     * @return array
     */
    protected static function inLineArray(array $records)
    {
        $values = [];
        foreach ($records as $record) {
            $values = array_merge($values, array_values($record));
        }

        return $values;
    }
}
