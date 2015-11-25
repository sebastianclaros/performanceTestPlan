package main

import (
	"github.com/stats/stats"
	"fmt"
	"strings"
	"io"
	"os"
	"encoding/csv"
	"strconv"
)

type Criteria struct {
	Aggregation string	
	Variable string	
	Operator string	
	Value float64	
}

var cmdExec = &Command{
	Run:   exec,
	Usage: "exec",
	Short: "performanceTestPlan exec <Measurement File> <Test Criteria>",
	Long: `
	Execute the Performance Test Plan define in the <Test Plan File> using the measures define in the <Measures File>. 
	The result will be display in the <Format> or by default will be text.


	<Measurement File> 
		CSV File with a header that have the variable names and the rest of the file are the set of measures

	<Test Criteria>
		The set of criterias that will validate. Will return true only if all the criterias are reach.
		Each criteria have the following format
		"<Aggregation>(<Variable>)<Operator><Value>"

		<Aggregation>: Represent the Statistic function that will be use in the criteria. 
		   The options are: count, sum, min, max, mean, stddev, var, kurtosis and skew
		<Variable>: Define which column of the file will use
		<Operator>: The options are: >, <, =, !=, >= or <=
		<Value>: Numeric Value that will be check in the criteria

	Examples:	
		performanceTestPlan exec measures.csv "sum(varA)<5" "max(varB)<5"

`,
}

func parseCriterias ( args []string ) []Criteria  {
	criterias := make([]Criteria, len(args))
	var err error

	for i := 0; i < len(args); i++ {
		c := strings.Trim( args[i], " \"")

		criteria := strings.Split(c, "(" )
		if len(criteria) == 2  {
			criterias[i].Aggregation = strings.ToLower(criteria[0])

			criteria := strings.Split(criteria[1], ")")
			if len(criteria) == 2  {
				criterias[i].Variable = strings.ToLower(criteria[0])

				pos := 1
				if criteria[1][1] == '=' {
					pos = 2
				}
				criterias[i].Operator = criteria[1][0:pos]
				criterias[i].Value,err = strconv.ParseFloat( criteria[1][pos:] , 10 )
				if err != nil  {
					ErrorAndExit ( "Value must be a valid number: %v ", criteria[1][pos:] )
				}
			} else {
				ErrorAndExit ( "Wrong expression: %v ", c)
			}
		} else {
			ErrorAndExit ( "Wrong expression: %v ", c)
		}
	}	
	return criterias
}

func exec( cmd *Command, args []string ) {
	criterias := parseCriterias( args[1:]) 

	ok := runExec( args[0], criterias )
	fmt.Println( args[0], ok )
}

/*
"EventType","SQOLRows","SQOLTime","SQOLNum","DMLRows","DMLTime","DMLNum","ExecTime","HEAPSize","TotalTime"
"ROOT",33,219707166,14,0,0,0,1064223497,0,1283930663
*/

func runExec ( measuresFile string, criterias []Criteria  ) bool {
	// Abre las medidas
	file, err := os.Open(measuresFile)
	if err != nil {
		ErrorAndExit ( "Cant open File %v", measuresFile)
	}
	// Arma un mapa con las estadisticas de cada medida
	r := csv.NewReader(file)
	header, _ := r.Read()
	mapStats := make(map[string] * stats.Stats, len(header))

	for i :=0 ; i < len(header) ; i++  {
		mapStats[ strings.ToLower(header[i]) ] = new (stats.Stats)
	}

	for err != io.EOF {
		record, err := r.Read()
		if err == nil {
			for i :=0 ; i < len(record) ; i++  {
		        value, err := strconv.ParseFloat( record[i] , 10 )
				if err == nil {
					s := mapStats[ strings.ToLower(header[i]) ];
					s.Update(value)
				}
			}			
		} else {
			break;
		}
	}

	for _, c := range ( criterias ) {
		s := mapStats[ c.Variable ]
		var value float64
		var result bool
		switch ( c.Aggregation ) {
			case "count":
				value = float64(s.Count())
			case "min":
				value = s.Min()
			case "max":
				value = s.Max()
			case "sum":
				value = s.Sum()
			case "mean":
				value = s.Mean()
			case "stddev":
				value = s.SampleStandardDeviation()
			case "var":
				value = s.SampleVariance()
			case "skew":
				value = s.SampleSkew()
			case "kurtosis":
				value = s.SampleKurtosis()
			default:
				ErrorAndExit ( "Unkown Aggregation: '%v'", c.Aggregation)
		}
		switch ( c.Operator ) {
			case ">":
				result = value > c.Value
			case ">=":
				result = value >= c.Value
			case "=":
				result = value == c.Value
			case "<":
				result = value < c.Value
			case "<=":
				result = value <= c.Value
			case "!=":
				result = value != c.Value
			default:
				ErrorAndExit ( "Unkown Aggregation: '%v'", c.Operator )				
		}
		if !result {
			return false
		}
	}
	return true
}

func init() {

}
