import { Parser } from 'node-sql-parser';
import { sync, Graph, ApiProperties } from 'ardoq-sdk-js';

type ArdoqCubeGraph = Graph<{}, {}>;

type CubeMeasure = {
  sql?: (...args: any) => string;
  type: string;
  drillMembers: any;
};
type CubeDimension = {
  sql?: (...args: any) => string;
  case?: any;
  type: string;
};
type Cube = {
  name: string;
  sql: () => string;
  measures: Record<string, CubeMeasure>;
  dimensions: Record<string, CubeDimension>;
};

type EvaluatedCubes = Record<string, Cube>;

type Edge = {
  from: string;
  to: string;
};

type Select = any;

const addEntitiesFromVisitedTable = (
  cubeName: string,
  visitedTable: any,
  components: any[],
  references: any[],
  visitedTables: Set<string>,
  referenceType = 'SELECT'
) => {
  if (!visitedTable.db) {
    return;
  }
  const tableKey = `${visitedTable.db}.${visitedTable.table}`;
  references.push({
    customId: `${cubeName}-${tableKey}`,
    source: cubeName,
    target: tableKey,
    type: referenceType,
  });
  if (!visitedTables.has(tableKey)) {
    visitedTables.add(tableKey);
    components.push({
      customId: tableKey,
      workspace: 'cubes',
      name: tableKey,
      type: 'Table',
      fields: [],
    });
  }
};

const addEntitiesFromSqlAST = (
  sourceName: string,
  parsedSql: Select,
  components: any[],
  references: any[],
  visitedTables: Set<string>
) => {
  if (!Array.isArray(parsedSql) && parsedSql.type === 'select') {
    parsedSql.from.forEach(visitedTable => {
      addEntitiesFromVisitedTable(
        sourceName,
        visitedTable,
        components,
        references,
        visitedTables
      );
    });
    if (parsedSql.with) {
      parsedSql.with.forEach(({ stmt }) => {
        addEntitiesFromSqlAST(
          sourceName,
          stmt.ast,
          components,
          references,
          visitedTables
        );
      });
    }
    if (parsedSql._next) {
      addEntitiesFromSqlAST(
        sourceName,
        parsedSql._next,
        components,
        references,
        visitedTables
      );
    }
  }
};

const getMeasureDescription = (measure: CubeMeasure) => {
  return `
### Calculation
    ${measure.sql ? measure.sql.toString() : measure.type}
`;
};

const getDimensionDescription = (dimension: CubeDimension) => {
  return `
### Calculation
    ${dimension.sql ? dimension.sql.toString() : ''}
    ${dimension.case ? JSON.stringify(dimension.case, null, 2) : ''}
`;
};

const getCubeId = (cubeName: string) => cubeName;
const getMeasureId = (cubeName: string, measureKey: string) =>
  `dimension-${cubeName}->${measureKey}`;
const getDimensionId = (cubeName: string, dimensionKey: string) =>
  `dimension-${cubeName}->${dimensionKey}`;

export const getGraphFromCube = (metaTransformer: any): ArdoqCubeGraph => {
  const joinGraph = metaTransformer.joinGraph;
  const evaluatedCubes = metaTransformer.cubeEvaluator
    .evaluatedCubes as EvaluatedCubes;
  const sqlParser = new Parser();
  const cubeComponents = Object.entries(evaluatedCubes).map(
    ([cubeKey, cube]) => {
      return {
        customId: cubeKey,
        workspace: 'cubes',
        name: cube.name,
        type: 'Cube',
        fields: [],
      };
    }
  );
  const references = Object.entries(joinGraph.edges).map(
    ([edgeKey, edge]: [string, Edge]) => ({
      customId: edgeKey,
      source: edge.from,
      target: edge.to,
      type: 'Joins',
    })
  );
  const dimensionComponents = Object.entries(evaluatedCubes).flatMap(
    ([, cube]: [string, Cube]) => {
      return Object.entries(cube.dimensions).map(
        ([dimensionKey, dimension]) => {
          const customId = getDimensionId(cube.name, dimensionKey);
          return {
            customId,
            workspace: 'cubes',
            parent: getCubeId(cube.name),
            name: dimensionKey,
            fields: [],
            type: 'Dimension',
            description: getDimensionDescription(dimension),
          };
        }
      );
    }
  );
  const measureComponents = Object.entries(evaluatedCubes).flatMap(
    ([, cube]: [string, Cube]) => {
      return Object.entries(cube.measures).map(([measureKey, measure]) => {
        const customId = getMeasureId(cube.name, measureKey);
        return {
          customId,
          workspace: 'cubes',
          parent: getCubeId(cube.name),
          name: measureKey,
          fields: [],
          type: 'Measure',
          description: getMeasureDescription(measure),
        };
      });
    }
  );
  const visitedTables = new Set<string>();
  const tableComponents = [];
  Object.values(evaluatedCubes).forEach(cube => {
    try {
      const parsedSql = sqlParser.astify(cube.sql());
      addEntitiesFromSqlAST(
        cube.name,
        parsedSql,
        tableComponents,
        references,
        visitedTables
      );
    } catch (e) {
      console.log(`ERROR WHEN PARSING ${cube.name}`, e.message);
      console.log(cube.sql());
    }
  });
  const graph = {
    components: [
      ...cubeComponents,
      ...dimensionComponents,
      ...measureComponents,
      ...tableComponents,
    ],
    references,
  };
  return graph;
};

export const syncCubeGraphToArdoq = (
  graph: ArdoqCubeGraph,
  apiProps: ApiProperties,
  workspaces: Record<string, string>
) => {
  sync(apiProps, workspaces, graph, []);
};
