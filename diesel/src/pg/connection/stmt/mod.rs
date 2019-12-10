extern crate pq_sys;

use std::ffi::CString;
use std::hash::{Hash, Hasher};
use std::os::raw as libc;
use std::ptr;

use super::result::PgResult;
use pg::{PgConnection, PgTypeMetadata};
use result::QueryResult;

pub use super::raw::RawConnection;

pub struct Statement {
    name: CString,
    sql_hash: String,
    param_formats: Vec<libc::c_int>,
}

impl Statement {
    #[allow(clippy::ptr_arg)]
    pub fn execute<'a>(
        &self,
        conn: &'a PgConnection,
        param_data: &Vec<Option<Vec<u8>>>,
    ) -> QueryResult<PgResult<'a>> {
        // Measure the operation
        let exec_span = debug_span!("execute", sql_hash = self.sql_hash.as_str(), name = ?self.name);  // NOTE: `.in_span`
        let _guard = exec_span.enter();

        let params_pointer = param_data
            .iter()
            .map(|data| {
                data.as_ref()
                    .map(|d| d.as_ptr() as *const libc::c_char)
                    .unwrap_or(ptr::null())
            })
            .collect::<Vec<_>>();
        let param_lengths = param_data
            .iter()
            .map(|data| data.as_ref().map(|d| d.len() as libc::c_int).unwrap_or(0))
            .collect::<Vec<_>>();
        let internal_res = unsafe {
            conn.raw_connection.exec_prepared(
                self.name.as_ptr(),
                params_pointer.len() as libc::c_int,
                params_pointer.as_ptr(),
                param_lengths.as_ptr(),
                self.param_formats.as_ptr(),
                1,
            )
        };

        PgResult::new(internal_res?, conn)
    }

    #[allow(clippy::ptr_arg)]
    pub fn prepare(
        conn: &PgConnection,
        sql: &str,
        name: Option<&str>,
        param_types: &[PgTypeMetadata],
    ) -> QueryResult<Self> {
        // Take the hash of the SQL
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        sql.hash(&mut hasher);
        let sql_hash = format!("{:?}", hasher.finish());

        info!(target: "prepare", sql, sql_hash = sql_hash.as_str(), ?param_types, ?name);

        let name = CString::new(name.unwrap_or(""))?;
        let sql = CString::new(sql)?;
        let param_types_vec = param_types.iter().map(|x| x.oid).collect();

        let internal_result = unsafe {
            conn.raw_connection.prepare(
                name.as_ptr(),
                sql.as_ptr(),
                param_types.len() as libc::c_int,
                param_types_to_ptr(Some(&param_types_vec)),
            )
        };
        PgResult::new(internal_result?, conn)?;

        Ok(Statement {
            name,
            sql_hash,
            param_formats: vec![1; param_types.len()],
        })
    }
}

fn param_types_to_ptr(param_types: Option<&Vec<u32>>) -> *const pq_sys::Oid {
    param_types
        .map(|types| types.as_ptr())
        .unwrap_or(ptr::null())
}
