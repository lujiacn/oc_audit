package oc_audit

import (
	"database/sql"
	"fmt"

	"github.com/lujiacn/oc_sql"
	_ "github.com/mattn/go-oci8"
)

type ChangeLog struct {
	DB          *sql.DB
	Study       string
	CutDate     string
	PtStart     string
	PtStop      string
	DcmName     string
	DciDcmRows  *sql.Rows
	ChangesList []interface{}
}

func (c *ChangeLog) GetChangeLog() (err error) {
	err = c.SetDciDcmRows()
	err = c.WriteChangesList()
	if err != nil {
		return err
	}
	return nil
}

//Query dci and dcm ids, and set to DciDCMRows
func (c *ChangeLog) SetDciDcmRows() (err error) {
	var w_pt, w_cutdate, w_dcm_name string

	if c.PtStart != "" && c.PtStop != "" {
		w_pt = fmt.Sprintf(`AND r_dcis.patient between %s and %s`, c.PtStart, c.PtStop)
	}

	if c.CutDate != "" {
		w_cutdate = fmt.Sprintf(`AND r_dcms.last_data_change_ts > TO_DATE('%s', 'YYYY-MM-DD')
        and R_DCMS.LAST_RESPONSE_MODIFICATION_TS > TO_DATE ('%s', ' YYYY-MM-DD') `, c.CutDate, c.CutDate)
	}

	if c.DcmName != "" {
		w_dcm_name = fmt.Sprintf("AND dcm.NAME='%s'", c.DcmName)
	}

	sql_str := fmt.Sprintf(`
               SELECT r_dcis.received_dci_id, r_dcms.received_dcm_id
              FROM received_dcis r_dcis,
                   received_dcms r_dcms,
                   dcms,
                   clinical_studies cs
             WHERE     r_dcis.clinical_study_id = cs.clinical_study_id
                   AND r_dcms.clinical_study_id = cs.clinical_study_id
                   AND dcms.clinical_study_id = cs.clinical_study_id
                   AND r_dcms.received_dci_id = r_dcis.received_dci_id
                   AND r_dcms.dcm_id = dcms.dcm_id
                   AND r_dcms.dcm_subset_sn = dcms.dcm_subset_sn
                   AND r_dcms.dcm_layout_sn = dcms.dcm_layout_sn
                   --AND dcms.NAME = 'test'
                   AND r_dcms.end_ts = TO_DATE (3000000, 'J')
                   AND r_dcis.end_ts = TO_DATE (3000000, 'J')
                   AND r_dcis.received_dci_status_code = 'PASS 1 COMPLETE'
                   AND r_dcis.received_dci_status_code <> 'BATCH LOADED'
                   AND r_dcis.clinical_study_id = cs.clinical_study_id
                   AND r_dcms.last_data_change_ts IS NOT NULL
                   AND cs.study = '%s' 
                    %s
                    %s
                    %s
                   and rownum < 10
                    `, c.Study, w_pt, w_cutdate, w_dcm_name)
	//write sql_str for DB check
	//fn := "dcm_sql.txt"
	//ioutil.WriteFile(fn, []byte(sql_str), os.ModeAppend)

	c.DciDcmRows, err = c.DB.Query(sql_str)
	return nil
}

func (c *ChangeLog) WriteChangesList() (err error) {

	for c.DciDcmRows.Next() {
		var dcm_id, dci_id int64
		c.DciDcmRows.Scan(&dci_id, &dcm_id)
		respRows := c.QueryResp(dci_id, dcm_id)
		colNames, _ := respRows.Columns()
		results := oc_sql.GetOcData(respRows)

		if len(c.ChangesList) == 0 {
			c.ChangesList = append(c.ChangesList, colNames)
		}

		if len(results[1:]) > 0 {
			for _, row := range results[1:] {
				c.ChangesList = append(c.ChangesList, row)
			}

		}
	}

	//fmt.Println(c.ChangesList)
	return nil
}

func (c *ChangeLog) QueryResp(dci_id, dcm_id int64) *sql.Rows {
	var w_cutdate string
	w_cutdate = fmt.Sprintf(`AND a.update_ts > TO_DATE('%s', 'YYYY-MM-DD')`, c.CutDate)
	sql_str := fmt.Sprintf(
		`SELECT DISTINCT a.patient, a.dcm_name, rdcm.visit_number, a.dcm_event,
                a.crf_name, a.q_name, a.repeat_sn, a.change_from, a.change_to,
                TO_CHAR (a.update_ts, 'YYYYMMDD HH:MM:SS') update_ts,
                a.changed_by, INITCAP (a.change_code) change_code,
                a.change_text, rdcm.document_number,
                TO_CHAR (a.response_id) response_id
                FROM rdc_audit_view a, received_dcms rdcm
                WHERE a.received_dci_id = %v
                AND a.received_dcm_id = %v
                AND rdcm.received_dcm_id = %v
                --AND INITCAP (a.change_code) NOT IN
                --                   ('Class. Change', 'Data Change', 'Derivation')
                AND NVL (a.change_code, '<None>') <> 'ROW RESEQUENCED'
                AND response_id <> 0
                %s
                ORDER BY rdcm.visit_number,
                a.dcm_name,
                a.dcm_event,
                a.crf_name,
                a.q_name,
                a.repeat_sn,
                update_ts
                `, dci_id, dcm_id, dcm_id, w_cutdate)
	//sql_str = fmt.Sprintf(sql_str, dci_id, dcm_id, dcm_id, w_cutdate)

	//write sql_str for DB check
	//fn := fmt.Sprintf("resp_sql_%v_%v.txt", dci_id, dcm_id)
	//ioutil.WriteFile(fn, []byte(sql_str), os.ModeAppend)

	rows, _ := c.DB.Query(sql_str)
	//results := oc_sql.GetOcData(rows)
	return rows
}
