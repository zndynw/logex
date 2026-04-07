mod app;
mod draw;

use crate::Result;
use crate::cli::TuiArgs;
use crate::config::Config;
use crossterm::event::{self, Event, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::layout::Rect;
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use std::io::{self, Stdout};
use std::path::Path;

pub use app::App;
use draw::draw;

pub fn run_tui(
    conn: &rusqlite::Connection,
    db_path: &Path,
    config: &Config,
    args: TuiArgs,
) -> Result<()> {
    let mut terminal = setup_terminal()?;
    let _guard = TerminalGuard;
    let mut app = App::new(
        db_path.to_path_buf(),
        db_path.display().to_string(),
        config.clone(),
        args,
    );
    app.refresh(conn)?;

    loop {
        app.poll_background(conn)?;
        let size = terminal.size()?;
        app.update_viewport(Rect::new(0, 0, size.width, size.height));
        terminal.draw(|frame| draw(frame, &app))?;

        let poll_timeout = app.poll_timeout();
        if event::poll(poll_timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press && app.handle_key(key)? {
                    break;
                }
            }
        }

        if app.should_refresh() {
            app.refresh(conn)?;
        }
    }

    Ok(())
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = io::stdout();
        let _ = execute!(stdout, LeaveAlternateScreen);
    }
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout)).map_err(Into::into)
}
