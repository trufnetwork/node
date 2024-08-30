package benchexport

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/fbiville/markdown-table-formatter/pkg/markdown"

	"golang.org/x/exp/slices"
)

type SaveAsMarkdownInput struct {
	Results      []SavedResults
	CurrentDate  time.Time
	InstanceType string
	FilePath     string
}

func SaveAsMarkdown(input SaveAsMarkdownInput) error {
	depths := make([]int, 0)
	days := make([]int, 0)

	for _, result := range input.Results {
		depths = append(depths, result.Depth)
		days = append(days, result.Days)
	}

	// remove duplicates
	slices.Sort(depths)
	slices.Sort(days)

	depths = slices.Compact(depths)
	days = slices.Compact(days)

	log.Printf("Saving to %s", input.FilePath)

	// Open the file in append mode, or create it if it doesn't exist
	file, err := os.OpenFile(input.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Check if the file is empty to determine whether to write the header
	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// check if all results have the same amount of samples. We proceed only if they are the same
	sampleCount := make(map[int]int)
	for _, result := range input.Results {
		sampleCount[result.Samples]++
	}
	if len(sampleCount) > 1 {
		return fmt.Errorf("results have different amount of samples")
	}

	// Write the header row only if the file is empty
	if stat.Size() == 0 {
		// Write the current date
		date := input.CurrentDate.Format("2006-01-02 15:04:05")
		_, err = file.WriteString(fmt.Sprintf("Date: %s\n\n## Dates x Depth\n\n", date))
		if err != nil {
			return err
		}
		// add how many samples
		_, err = file.WriteString(fmt.Sprintf("Samples per query: %d\n", input.Results[0].Samples))
		if err != nil {
			return err
		}
		_, err = file.WriteString("Results in milliseconds\n\n")
		if err != nil {
			return err
		}
	}

	// Group results by [instance_type][procedure][visibility]
	groupedResults := make(map[string]map[string]map[string]map[int]map[int]int64)
	for _, result := range input.Results {
		instanceType := input.InstanceType
		procedure := result.Procedure
		visibility := result.Visibility
		if _, ok := groupedResults[instanceType]; !ok {
			groupedResults[instanceType] = make(map[string]map[string]map[int]map[int]int64)
		}
		if _, ok := groupedResults[instanceType][procedure]; !ok {
			groupedResults[instanceType][procedure] = make(map[string]map[int]map[int]int64)
		}
		if _, ok := groupedResults[instanceType][procedure][visibility]; !ok {
			groupedResults[instanceType][procedure][visibility] = make(map[int]map[int]int64)
		}
		if _, ok := groupedResults[instanceType][procedure][visibility][result.Days]; !ok {
			groupedResults[instanceType][procedure][visibility][result.Days] = make(map[int]int64)
		}
		groupedResults[instanceType][procedure][visibility][result.Days][result.Depth] = result.DurationMs
	}

	// Sort instance types to ensure consistent order
	instanceTypes := make([]string, 0, len(groupedResults))
	for instanceType := range groupedResults {
		instanceTypes = append(instanceTypes, instanceType)
	}
	slices.Sort(instanceTypes)

	// Write markdown for each instance type, procedure, and visibility combination
	for _, instanceType := range instanceTypes {
		if _, err = file.WriteString(fmt.Sprintf("### %s\n\n", instanceType)); err != nil {
			return err
		}

		procedures := groupedResults[instanceType]

		// sort procedures
		proceduresKeys := make([]string, 0, len(procedures))
		for procedure := range procedures {
			proceduresKeys = append(proceduresKeys, procedure)
		}
		slices.Sort(proceduresKeys)

		for _, procedure := range proceduresKeys {
			visibilities := procedures[procedure]
			visibilitiesKeys := make([]string, 0, len(visibilities))
			for visibility := range visibilities {
				visibilitiesKeys = append(visibilitiesKeys, visibility)
			}
			slices.Sort(visibilitiesKeys)

			for _, visibility := range visibilitiesKeys {
				daysMap := visibilities[visibility]

				// Write full information for each table
				if _, err = file.WriteString(fmt.Sprintf("%s - %s - %s \n\n", instanceType, procedure, visibility)); err != nil {
					return err
				}

				// Create headers for the table
				headers := []string{"queried days / depth"}
				for _, depth := range depths {
					headers = append(headers, fmt.Sprintf("%d", depth))
				}

				// Create a new table formatter
				tableFormatter := markdown.NewTableFormatterBuilder().
					WithPrettyPrint().
					Build(headers...)

				rows := make([][]string, 0)

				// Add rows for each day
				for _, day := range days {
					exists := false
					row := []string{fmt.Sprintf("%d", day)}
					for _, depth := range depths {
						if duration, ok := daysMap[day][depth]; ok {
							row = append(row, fmt.Sprintf("%d", duration))
							exists = true
						} else {
							row = append(row, "")
						}
					}
					if exists {
						rows = append(rows, row)
					}
				}

				// Format the table
				formattedTable, err := tableFormatter.Format(rows)
				if err != nil {
					return err
				}

				// Write the formatted table to the file
				if _, err = file.WriteString(formattedTable + "\n\n"); err != nil {
					return err
				}
			}

			// Add an extra newline between procedures for better readability
			if _, err = file.WriteString("\n"); err != nil {
				return err
			}
		}
	}

	return nil
}
