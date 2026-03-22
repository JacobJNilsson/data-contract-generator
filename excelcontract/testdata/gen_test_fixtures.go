//go:build ignore

// Generate test fixtures for excelcontract tests.
// Run: go run excelcontract/testdata/gen_test_fixtures.go
package main

import (
	"fmt"
	"log"

	"github.com/xuri/excelize/v2"
)

func main() {
	genSimple()
	genMultiSheet()
	genNoHeader()
	genExcelTable()
	genEmpty()
	genTypes()
	genSingleColumn()
	fmt.Println("All test fixtures generated.")
}

func genSimple() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetName("Sheet1", "People")
	f.SetSheetRow("People", "A1", &[]any{"Name", "Age", "City"})
	f.SetSheetRow("People", "A2", &[]any{"Alice", 30, "New York"})
	f.SetSheetRow("People", "A3", &[]any{"Bob", 25, "London"})
	f.SetSheetRow("People", "A4", &[]any{"Charlie", 35, "Paris"})
	f.SetSheetRow("People", "A5", &[]any{"Diana", 28, "Berlin"})
	f.SetSheetRow("People", "A6", &[]any{"Eve", 32, "Tokyo"})

	must(f.SaveAs("excelcontract/testdata/simple.xlsx"))
	fmt.Println("  simple.xlsx")
}

func genMultiSheet() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetName("Sheet1", "Users")
	f.SetSheetRow("Users", "A1", &[]any{"ID", "Username", "Email"})
	f.SetSheetRow("Users", "A2", &[]any{1, "alice", "alice@example.com"})
	f.SetSheetRow("Users", "A3", &[]any{2, "bob", "bob@example.com"})

	f.NewSheet("Products")
	f.SetSheetRow("Products", "A1", &[]any{"SKU", "Name", "Price", "Stock"})
	f.SetSheetRow("Products", "A2", &[]any{"P001", "Widget", 9.99, 100})
	f.SetSheetRow("Products", "A3", &[]any{"P002", "Gadget", 24.50, 50})
	f.SetSheetRow("Products", "A4", &[]any{"P003", "Doohickey", 4.75, 200})

	f.NewSheet("Orders")
	f.SetSheetRow("Orders", "A1", &[]any{"OrderID", "UserID", "SKU", "Quantity"})
	f.SetSheetRow("Orders", "A2", &[]any{1001, 1, "P001", 3})
	f.SetSheetRow("Orders", "A3", &[]any{1002, 2, "P002", 1})

	must(f.SaveAs("excelcontract/testdata/multi-sheet.xlsx"))
	fmt.Println("  multi-sheet.xlsx")
}

func genNoHeader() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{1, 2, 3})
	f.SetSheetRow("Sheet1", "A2", &[]any{4, 5, 6})
	f.SetSheetRow("Sheet1", "A3", &[]any{7, 8, 9})

	must(f.SaveAs("excelcontract/testdata/no-header.xlsx"))
	fmt.Println("  no-header.xlsx")
}

func genExcelTable() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{"Product", "Revenue", "Region"})
	f.SetSheetRow("Sheet1", "A2", &[]any{"Widget", 1500.00, "North"})
	f.SetSheetRow("Sheet1", "A3", &[]any{"Gadget", 2300.50, "South"})
	f.SetSheetRow("Sheet1", "A4", &[]any{"Doohickey", 800.75, "East"})

	must(f.AddTable("Sheet1", &excelize.Table{
		Range: "A1:C4",
		Name:  "SalesTable",
	}))

	must(f.SaveAs("excelcontract/testdata/excel-table.xlsx"))
	fmt.Println("  excel-table.xlsx")
}

func genEmpty() {
	f := excelize.NewFile()
	defer f.Close()

	must(f.SaveAs("excelcontract/testdata/empty.xlsx"))
	fmt.Println("  empty.xlsx")
}

func genTypes() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{"Text", "Integer", "Decimal", "Date", "Boolean"})
	f.SetSheetRow("Sheet1", "A2", &[]any{"hello", 42, 3.14, "2024-01-15", true})
	f.SetSheetRow("Sheet1", "A3", &[]any{"world", 100, 2.71, "2024-06-30", false})
	f.SetSheetRow("Sheet1", "A4", &[]any{"foo", 7, 1.41, "2024-12-25", true})

	must(f.SaveAs("excelcontract/testdata/types.xlsx"))
	fmt.Println("  types.xlsx")
}

func genSingleColumn() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetCellValue("Sheet1", "A1", "Value")
	f.SetCellValue("Sheet1", "A2", 42)

	must(f.SaveAs("excelcontract/testdata/single-column.xlsx"))
	fmt.Println("  single-column.xlsx")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
