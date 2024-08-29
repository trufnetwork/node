package benchexport

import (
	"fmt"
	"os"
	"time"

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

	// Write the header row only if the file is empty
	if stat.Size() == 0 {
		// Write the current date
		date := input.CurrentDate.Format("2006-01-02 15:04:05")
		_, err = file.WriteString(fmt.Sprintf("Date: %s\n\n## Dates x Depth\n\n", date))
		if err != nil {
			return err
		}
	}

	// Group results by [procedure][visibility]
	groupedResults := make(map[string]map[string]map[int]map[int]int64)
	for _, result := range input.Results {
		procedure := result.Procedure
		visibility := result.Visibility
		if _, ok := groupedResults[procedure]; !ok {
			groupedResults[procedure] = make(map[string]map[int]map[int]int64)
		}
		if _, ok := groupedResults[procedure][visibility]; !ok {
			groupedResults[procedure][visibility] = make(map[int]map[int]int64)
		}
		if _, ok := groupedResults[procedure][visibility][result.Days]; !ok {
			groupedResults[procedure][visibility][result.Days] = make(map[int]int64)
		}
		groupedResults[procedure][visibility][result.Days][result.Depth] = result.DurationMs
	}

	// Write markdown for each procedure and visibility combination
	for procedure, visibilities := range groupedResults {
		for visibility, daysMap := range visibilities {
			if _, err = file.WriteString(fmt.Sprintf("%s - %s - %s\n\n", input.InstanceType, procedure, visibility)); err != nil {
				return err
			}

			// Write table header
			_, err = file.WriteString("| queried days / depth |")

			if err != nil {
				return err
			}

			for _, depth := range depths {
				if _, err = file.WriteString(fmt.Sprintf(" %d |", depth)); err != nil {
					return err
				}
			}
			_, err = file.WriteString("\n|----------------------|")
			if err != nil {
				return err
			}
			for range depths {
				if _, err = file.WriteString("---|"); err != nil {
					return err
				}
			}
			_, err = file.WriteString("\n")
			if err != nil {
				return err
			}

			// Write table rows
			for _, day := range days {
				row := fmt.Sprintf("| %d ", day)
				for _, depth := range depths {
					if duration, ok := daysMap[day][depth]; ok {
						row += fmt.Sprintf("| %d ", duration)
					} else {
						row += "|    "
					}
				}
				row += "|\n"
				if _, err = file.WriteString(row); err != nil {
					return err
				}
			}

			if _, err = file.WriteString("\n"); err != nil {
				return err
			}
		}
	}

	return nil
}
