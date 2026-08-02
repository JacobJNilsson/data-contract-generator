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
	genMultiTableStacked()
	genTitledTable()
	genTwoTableObjects()
	genTablePlusNotes()
	genHeaderlessTable()
	genMergedCaption()
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

// genMultiTableStacked writes two blank-row-separated tables in one
// sheet: the XL-1 unit-per-table case.
func genMultiTableStacked() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{"Product", "Qty", "Price"})
	f.SetSheetRow("Sheet1", "A2", &[]any{"Widget", 4, 9.99})
	f.SetSheetRow("Sheet1", "A3", &[]any{"Gadget", 2, 24.50})
	// Row 4 is blank: the separator.
	f.SetSheetRow("Sheet1", "A5", &[]any{"Name", "Team"})
	f.SetSheetRow("Sheet1", "A6", &[]any{"Alice", "Platform"})
	f.SetSheetRow("Sheet1", "A7", &[]any{"Bob", "Data"})
	f.SetSheetRow("Sheet1", "A8", &[]any{"Carol", "Platform"})

	must(f.SaveAs("excelcontract/testdata/multi-table-stacked.xlsx"))
	fmt.Println("  multi-table-stacked.xlsx")
}

// genTitledTable writes a one-cell caption above a table, in the same
// band: the caption-absorption case.
func genTitledTable() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetCellValue("Sheet1", "A1", "Kontoutdrag 2024")
	f.SetSheetRow("Sheet1", "A2", &[]any{"Datum", "Text", "Belopp"})
	f.SetSheetRow("Sheet1", "A3", &[]any{"2024-01-02", "Lön", 32000})
	f.SetSheetRow("Sheet1", "A4", &[]any{"2024-01-05", "Hyra", -11500})
	f.SetSheetRow("Sheet1", "A5", &[]any{"2024-01-09", "Mat", -843})

	must(f.SaveAs("excelcontract/testdata/titled-table.xlsx"))
	fmt.Println("  titled-table.xlsx")
}

// genTwoTableObjects writes two declared Table objects plus a heuristic
// table and a stray cell outside every range.
func genTwoTableObjects() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{"Product", "Revenue", "Region"})
	f.SetSheetRow("Sheet1", "A2", &[]any{"Widget", 1500.00, "North"})
	f.SetSheetRow("Sheet1", "A3", &[]any{"Gadget", 2300.50, "South"})
	f.SetSheetRow("Sheet1", "A4", &[]any{"Doohickey", 800.75, "East"})
	must(f.AddTable("Sheet1", &excelize.Table{Range: "A1:C4", Name: "Sales"}))

	f.SetSheetRow("Sheet1", "E1", &[]any{"Quarter", "Target"})
	f.SetSheetRow("Sheet1", "E2", &[]any{"Q1", 5000})
	f.SetSheetRow("Sheet1", "E3", &[]any{"Q2", 6000})
	f.SetSheetRow("Sheet1", "E4", &[]any{"Q3", 6500})
	must(f.AddTable("Sheet1", &excelize.Table{Range: "E1:F4", Name: "Targets"}))

	f.SetSheetRow("Sheet1", "A7", &[]any{"Note", "Author"})
	f.SetSheetRow("Sheet1", "A8", &[]any{"Preliminary", "JN"})

	f.SetCellValue("Sheet1", "D12", "reviewed 2026-07-01")

	must(f.SaveAs("excelcontract/testdata/two-table-objects.xlsx"))
	fmt.Println("  two-table-objects.xlsx")
}

// genTablePlusNotes writes a table plus a stray single cell below it:
// the noise-guard case.
func genTablePlusNotes() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{"Item", "Count"})
	f.SetSheetRow("Sheet1", "A2", &[]any{"Bolt", 120})
	f.SetSheetRow("Sheet1", "A3", &[]any{"Nut", 118})
	// Row 4 is blank; the note stands alone.
	f.SetCellValue("Sheet1", "A5", "counts are from the spring audit")

	must(f.SaveAs("excelcontract/testdata/table-plus-notes.xlsx"))
	fmt.Println("  table-plus-notes.xlsx")
}

// genHeaderlessTable writes rows whose first row is data, so header
// detection reports no header and names are synthesized.
func genHeaderlessTable() {
	f := excelize.NewFile()
	defer f.Close()

	f.SetSheetRow("Sheet1", "A1", &[]any{"P-600", 75.50, "2026-06-07"})
	f.SetSheetRow("Sheet1", "A2", &[]any{"P-601", 189.00, "2026-06-07"})
	f.SetSheetRow("Sheet1", "A3", &[]any{"P-602", 45.25, "2026-06-08"})

	must(f.SaveAs("excelcontract/testdata/headerless-table.xlsx"))
	fmt.Println("  headerless-table.xlsx")
}

// genMergedCaption writes a merged A1:D1 title above a table: merged
// cells report their value in the top-left cell only (XL-L2), which is
// exactly the single-cell shape caption absorption peels.
func genMergedCaption() {
	f := excelize.NewFile()
	defer f.Close()

	must(f.MergeCell("Sheet1", "A1", "D1"))
	f.SetCellValue("Sheet1", "A1", "Quarterly Report 2025")
	f.SetSheetRow("Sheet1", "A2", &[]any{"Region", "Sales", "Cost", "Margin"})
	f.SetSheetRow("Sheet1", "A3", &[]any{"North", 1200, 700, 500})
	f.SetSheetRow("Sheet1", "A4", &[]any{"South", 900, 600, 300})

	must(f.SaveAs("excelcontract/testdata/merged-caption.xlsx"))
	fmt.Println("  merged-caption.xlsx")
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
