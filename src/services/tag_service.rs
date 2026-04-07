use crate::Result;
use crate::cli::{TagsArgs, TagsOutput};
use crate::filters::TagListFilter;
use crate::formatter::{TagRow, print_tags_rows_table};
use crate::store::fetch_tag_rows;
use crate::utils::json_escape;
use rusqlite::Connection;

pub fn handle_tags(conn: &Connection, args: TagsArgs) -> Result<()> {
    let filter = TagListFilter::from_tags_args(&args)?;
    let tag_rows = query_tag_rows(conn, &filter)?;

    if tag_rows.is_empty() {
        println!("no tags found");
        return Ok(());
    }

    match args.output {
        TagsOutput::Table => print_tags_rows_table(&tag_rows),
        TagsOutput::Plain => {
            for row in &tag_rows {
                println!("{} ({})", row.tag, row.task_count);
            }
        }
        TagsOutput::Json => {
            println!("[");
            for (i, row) in tag_rows.iter().enumerate() {
                println!(
                    "  {{\"tag\":\"{}\",\"count\":{},\"last_task_id\":{},\"last_started_at\":\"{}\"}}{}",
                    json_escape(&row.tag),
                    row.task_count,
                    row.last_task_id,
                    json_escape(&row.last_started_at),
                    if i < tag_rows.len() - 1 { "," } else { "" }
                );
            }
            println!("]");
        }
    }

    Ok(())
}

pub fn query_tag_rows(conn: &Connection, filter: &TagListFilter) -> Result<Vec<TagRow>> {
    fetch_tag_rows(conn, filter)
}
