use crate::Result;
use crate::analyzer::{AnalysisFilter, AnalysisReport, collect_analysis, render_analysis_json, render_analysis_plain};
use crate::cli::AnalyzeArgs;
use crate::filters::AnalysisRequest;
use rusqlite::Connection;

pub fn handle_analyze(conn: &Connection, args: AnalyzeArgs) -> Result<()> {
    let request = AnalysisRequest::from_analyze_args(&args)?;
    let filter = AnalysisFilter {
        tag: request.tag,
        from: request.time_range.from,
        to: request.time_range.to,
        top_tags: request.top_tags,
    };

    let analysis = collect_analysis(conn, &filter)?;
    println!("{}", render_analyze_output(&analysis, args.json));

    Ok(())
}

pub fn render_analyze_output(analysis: &AnalysisReport, json: bool) -> String {
    if json {
        render_analysis_json(analysis)
    } else {
        render_analysis_plain(analysis)
    }
}
