use anyhow::{anyhow, Result};
use clap::Parser;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use efs::crypto::standard::{StandardCipher, StandardHasher, StandardKdf};
use efs::mirror::MirrorOrchestrator;
use efs::silo::SiloManager;
use efs::storage::local::LocalBackend;
use efs::storage::s3::S3Backend;
use efs::{Efs, StorageBackend};
use ratatui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Direction, Layout},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph},
    Frame, Terminal,
};
use std::collections::HashSet;
use std::io;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod config;
use config::{BackendConfig, BackendType, Config};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the configuration file
    #[arg(short, long, default_value = "efs_config.bin")]
    config: String,
}

enum AppState {
    Login,
    Main,
    ManageBackends,
    AddBackend(AddBackendState),
    ManageSilos,
    AddSilo {
        input: String,
    },
    PromptInitSilo {
        silo_id: String,
    },
    Upload {
        local_path: String,
        remote_path: String,
        current_input: usize,
    },
    Download {
        remote_path: String,
        local_path: String,
    },
    ConfirmDelete {
        path: String,
    },
    Move {
        source_path: String,
        dest_path: String,
    },
    Deleting,
    Moving,
    Error(String),
}

struct AddBackendState {
    backend_type: NewBackendType,
    current_input: usize,
    inputs: Vec<String>,
}

enum NewBackendType {
    Local,
    S3,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum FileItem {
    Dir(String),
    File(String),
}

struct App {
    state: AppState,
    config_path: String,
    password: String,
    config: Option<Config>,
    files: Vec<String>,
    current_dir: String,
    file_list_state: ListState,
    backend_list_state: ListState,
    silo_list_state: ListState,
    efs: Option<Efs>,
    silo_id: String,
}

impl App {
    fn new(config_path: String) -> Self {
        Self {
            state: AppState::Login,
            config_path,
            password: String::new(),
            config: None,
            files: Vec::new(),
            current_dir: "/".to_string(),
            file_list_state: ListState::default(),
            backend_list_state: ListState::default(),
            silo_list_state: ListState::default(),
            efs: None,
            silo_id: String::new(),
        }
    }

    async fn login(&mut self) -> Result<()> {
        let cfg = config::load_config(&self.config_path, self.password.as_bytes())?;
        self.config = Some(cfg.clone());

        if let Some(first_silo) = cfg.known_silos.first() {
            self.silo_id = first_silo.clone();
            if self.rebuild_efs().await.is_err() {
                self.state = AppState::PromptInitSilo {
                    silo_id: self.silo_id.clone(),
                };
                return Ok(());
            }
        }

        self.state = AppState::Main;
        Ok(())
    }

    async fn rebuild_efs(&mut self) -> Result<()> {
        let cfg = self
            .config
            .as_ref()
            .ok_or_else(|| anyhow!("Config not loaded"))?;

        if cfg.backends.is_empty() || self.silo_id.is_empty() {
            self.efs = None;
            self.files = Vec::new();
            return Ok(());
        }

        let storage = get_storage(cfg).await?;
        let manager = SiloManager::new(
            Box::new(StandardKdf),
            Box::new(StandardCipher),
            Box::new(StandardHasher),
        );

        let silo_cfg = manager
            .load_silo(storage.as_ref(), self.password.as_bytes(), &self.silo_id)
            .await?;

        let efs = Efs::new(
            storage,
            Arc::new(StandardCipher),
            silo_cfg.data_key,
            silo_cfg.chunk_size,
        )
        .await?;

        self.files = efs.list().await?;
        self.efs = Some(efs);
        Ok(())
    }

    async fn refresh_files(&mut self) -> Result<()> {
        if let Some(efs) = &self.efs {
            self.files = efs.list().await?;
        }
        Ok(())
    }

    fn save_config(&self) -> Result<()> {
        if let Some(cfg) = &self.config {
            config::save_config(&self.config_path, cfg, self.password.as_bytes())?;
        }
        Ok(())
    }

    fn add_silo(&mut self, silo_id: &str) -> Result<()> {
        let cfg = self
            .config
            .as_mut()
            .ok_or_else(|| anyhow!("Config not loaded"))?;

        if !cfg.known_silos.contains(&silo_id.to_string()) {
            cfg.known_silos.push(silo_id.to_string());
        }
        self.save_config()?;
        Ok(())
    }

    async fn initialize_new_silo(&mut self, silo_id: &str) -> Result<()> {
        let cfg = self
            .config
            .as_mut()
            .ok_or_else(|| anyhow!("Config not loaded"))?;
        let storage = get_storage(cfg).await?;
        let manager = SiloManager::new(
            Box::new(StandardKdf),
            Box::new(StandardCipher),
            Box::new(StandardHasher),
        );

        let mut data_key = vec![0u8; 32];
        use rand::RngCore;
        rand::thread_rng().fill_bytes(&mut data_key);

        manager
            .initialize_silo(
                storage.as_ref(),
                self.password.as_bytes(),
                silo_id,
                cfg.chunk_size,
                data_key,
            )
            .await?;

        self.add_silo(silo_id)?;
        Ok(())
    }

    fn get_current_view(&self) -> Vec<FileItem> {
        if self.efs.is_none() {
            return Vec::new();
        }
        let mut items = HashSet::new();
        let mut prefix = self.current_dir.clone();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        for path in &self.files {
            let remaining = if path.starts_with(&prefix) {
                &path[prefix.len()..]
            } else if prefix.starts_with('/') && path.starts_with(&prefix[1..]) {
                &path[prefix.len() - 1..]
            } else {
                continue;
            };

            if remaining.is_empty() {
                continue;
            }

            if let Some(slash_idx) = remaining.find('/') {
                let dir_name = &remaining[..slash_idx];
                if !dir_name.is_empty() {
                    items.insert(FileItem::Dir(dir_name.to_string()));
                }
            } else {
                items.insert(FileItem::File(remaining.to_string()));
            }
        }

        let mut sorted_items: Vec<FileItem> = items.into_iter().collect();
        sorted_items.sort();

        if self.current_dir != "/" {
            sorted_items.insert(0, FileItem::Dir("..".to_string()));
        }

        sorted_items
    }

    fn get_full_path(&self, item: &FileItem) -> String {
        let name = match item {
            FileItem::Dir(n) => n,
            FileItem::File(n) => n,
        };

        if name == ".." {
            if let Some(idx) = self.current_dir.trim_end_matches('/').rfind('/') {
                let mut p = self.current_dir[..idx].to_string();
                if p.is_empty() {
                    p = "/".to_string();
                }
                return p;
            }
            return "/".to_string();
        }

        let mut p = self.current_dir.clone();
        if !p.ends_with('/') {
            p.push('/');
        }
        p.push_str(name);
        p
    }
}

async fn get_storage(cfg: &Config) -> Result<Arc<dyn StorageBackend>> {
    if cfg.backends.is_empty() {
        return Err(anyhow!("No backends configured."));
    }

    let mut backends = Vec::new();

    for b_cfg in &cfg.backends {
        match &b_cfg.backend_type {
            BackendType::S3 {
                bucket,
                region,
                access_key,
                secret_key,
            } => {
                use object_store::aws::AmazonS3Builder;
                let mut builder = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_region(region);

                if let Some(key) = access_key {
                    builder = builder.with_access_key_id(key);
                }
                if let Some(secret) = secret_key {
                    builder = builder.with_secret_access_key(secret);
                }

                let store = builder.build()?;
                let backend = S3Backend::new(Arc::new(store));
                backends.push(Box::new(backend) as Box<dyn StorageBackend>);
            }
            BackendType::Local { path } => {
                let backend = LocalBackend::new(path)?;
                backends.push(Box::new(backend) as Box<dyn StorageBackend>);
            }
        }
    }

    Ok(Arc::new(MirrorOrchestrator::new(backends)))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = App::new(args.config);
    let res = run_app(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    if let Err(err) = res {
        eprintln!("{:?}", err)
    }

    Ok(())
}

async fn run_app<B: Backend>(terminal: &mut Terminal<B>, app: &mut App) -> io::Result<()> {
    let tick_rate = Duration::from_millis(250);
    let mut last_tick = Instant::now();
    loop {
        terminal.draw(|f| ui(f, app))?;

        let timeout = tick_rate
            .checked_sub(last_tick.elapsed())
            .unwrap_or_else(|| Duration::from_secs(0));
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                match &mut app.state {
                    AppState::Login => match key.code {
                        KeyCode::Enter => {
                            if let Err(e) = app.login().await {
                                app.state = AppState::Error(e.to_string());
                            }
                        }
                        KeyCode::Char(c) => {
                            app.password.push(c);
                        }
                        KeyCode::Backspace => {
                            app.password.pop();
                        }
                        KeyCode::Esc => return Ok(()),
                        _ => {}
                    },
                    AppState::Main => {
                        let current_view = app.get_current_view();
                        match key.code {
                            KeyCode::Char('q') => return Ok(()),
                            KeyCode::Char('b') => {
                                app.state = AppState::ManageBackends;
                            }
                            KeyCode::Char('s') => {
                                app.state = AppState::ManageSilos;
                            }
                            KeyCode::Char('r') => {
                                if let Err(e) = app.refresh_files().await {
                                    app.state = AppState::Error(e.to_string());
                                }
                            }
                            KeyCode::Char('u') if app.efs.is_some() => {
                                let mut remote_path = app.current_dir.clone();
                                if !remote_path.ends_with('/') {
                                    remote_path.push('/');
                                }
                                app.state = AppState::Upload {
                                    local_path: String::new(),
                                    remote_path,
                                    current_input: 0,
                                };
                            }
                            KeyCode::Char('d') if app.efs.is_some() => {
                                if let Some(i) = app.file_list_state.selected() {
                                    if let Some(FileItem::File(name)) = current_view.get(i) {
                                        let full_path =
                                            app.get_full_path(&FileItem::File(name.clone()));
                                        app.state = AppState::Download {
                                            remote_path: full_path,
                                            local_path: name.clone(),
                                        };
                                    }
                                }
                            }
                            KeyCode::Char('x') if app.efs.is_some() => {
                                if let Some(i) = app.file_list_state.selected() {
                                    if let Some(item) = current_view.get(i) {
                                        let full_path = app.get_full_path(item);
                                        app.state = AppState::ConfirmDelete { path: full_path };
                                    }
                                }
                            }
                            KeyCode::Char('m') if app.efs.is_some() => {
                                if let Some(i) = app.file_list_state.selected() {
                                    if let Some(item) = current_view.get(i) {
                                        if let FileItem::File(_) = item {
                                            let full_path = app.get_full_path(item);
                                            app.state = AppState::Move {
                                                source_path: full_path.clone(),
                                                dest_path: full_path,
                                            };
                                        }
                                    }
                                }
                            }
                            KeyCode::Enter => {
                                if let Some(i) = app.file_list_state.selected() {
                                    if let Some(item) = current_view.get(i) {
                                        match item {
                                            FileItem::Dir(name) => {
                                                if name == ".." {
                                                    if let Some(idx) = app
                                                        .current_dir
                                                        .trim_end_matches('/')
                                                        .rfind('/')
                                                    {
                                                        app.current_dir =
                                                            app.current_dir[..idx].to_string();
                                                        if app.current_dir.is_empty() {
                                                            app.current_dir = "/".to_string();
                                                        }
                                                    }
                                                } else {
                                                    let mut new_dir = app.current_dir.clone();
                                                    if !new_dir.ends_with('/') {
                                                        new_dir.push('/');
                                                    }
                                                    new_dir.push_str(name);
                                                    app.current_dir = new_dir;
                                                }
                                                app.file_list_state.select(Some(0));
                                            }
                                            FileItem::File(_) => {}
                                        }
                                    }
                                }
                            }
                            KeyCode::Down => {
                                let i = match app.file_list_state.selected() {
                                    Some(i) if !current_view.is_empty() => {
                                        (i + 1) % current_view.len()
                                    }
                                    _ => 0,
                                };
                                if !current_view.is_empty() {
                                    app.file_list_state.select(Some(i));
                                }
                            }
                            KeyCode::Up => {
                                let i = match app.file_list_state.selected() {
                                    Some(i) if !current_view.is_empty() => {
                                        (i + current_view.len() - 1) % current_view.len()
                                    }
                                    _ => 0,
                                };
                                if !current_view.is_empty() {
                                    app.file_list_state.select(Some(i));
                                }
                            }
                            _ => {}
                        }
                    }
                    AppState::ManageBackends => match key.code {
                        KeyCode::Esc => {
                            app.state = AppState::Main;
                        }
                        KeyCode::Char('a') => {
                            app.state = AppState::AddBackend(AddBackendState {
                                backend_type: NewBackendType::Local,
                                current_input: 0,
                                inputs: vec![String::new(), String::new()],
                            });
                        }
                        KeyCode::Char('s') => {
                            app.state = AppState::AddBackend(AddBackendState {
                                backend_type: NewBackendType::S3,
                                current_input: 0,
                                inputs: vec![
                                    String::new(),
                                    String::new(),
                                    String::new(),
                                    String::new(),
                                    String::new(),
                                ],
                            });
                        }
                        KeyCode::Char('d') => {
                            if let Some(i) = app.backend_list_state.selected() {
                                if let Some(cfg) = &mut app.config {
                                    if i < cfg.backends.len() {
                                        cfg.backends.remove(i);
                                        if let Err(e) = app.save_config() {
                                            app.state = AppState::Error(e.to_string());
                                        } else {
                                            let _ = app.rebuild_efs().await;
                                        }
                                    }
                                }
                            }
                        }
                        KeyCode::Down => {
                            if let Some(cfg) = &app.config {
                                let i = match app.backend_list_state.selected() {
                                    Some(i) if !cfg.backends.is_empty() => {
                                        (i + 1) % cfg.backends.len()
                                    }
                                    _ => 0,
                                };
                                if !cfg.backends.is_empty() {
                                    app.backend_list_state.select(Some(i));
                                }
                            }
                        }
                        KeyCode::Up => {
                            if let Some(cfg) = &app.config {
                                let i = match app.backend_list_state.selected() {
                                    Some(i) if !cfg.backends.is_empty() => {
                                        (i + cfg.backends.len() - 1) % cfg.backends.len()
                                    }
                                    _ => 0,
                                };
                                if !cfg.backends.is_empty() {
                                    app.backend_list_state.select(Some(i));
                                }
                            }
                        }
                        _ => {}
                    },
                    AppState::AddBackend(st) => match key.code {
                        KeyCode::Esc => {
                            app.state = AppState::ManageBackends;
                        }
                        KeyCode::Tab | KeyCode::Down => {
                            st.current_input = (st.current_input + 1) % st.inputs.len();
                        }
                        KeyCode::Up => {
                            st.current_input =
                                (st.current_input + st.inputs.len() - 1) % st.inputs.len();
                        }
                        KeyCode::Enter => {
                            let name = st.inputs[0].clone();
                            let b_type = match st.backend_type {
                                NewBackendType::Local => BackendType::Local {
                                    path: st.inputs[1].clone(),
                                },
                                NewBackendType::S3 => BackendType::S3 {
                                    bucket: st.inputs[1].clone(),
                                    region: st.inputs[2].clone(),
                                    access_key: if st.inputs[3].is_empty() {
                                        None
                                    } else {
                                        Some(st.inputs[3].clone())
                                    },
                                    secret_key: if st.inputs[4].is_empty() {
                                        None
                                    } else {
                                        Some(st.inputs[4].clone())
                                    },
                                },
                            };
                            if let Some(cfg) = &mut app.config {
                                cfg.backends.push(BackendConfig {
                                    name,
                                    backend_type: b_type,
                                });
                                if let Err(e) = app.save_config() {
                                    app.state = AppState::Error(e.to_string());
                                } else {
                                    let _ = app.rebuild_efs().await;
                                    app.state = AppState::ManageBackends;
                                }
                            }
                        }
                        KeyCode::Char(c) => {
                            st.inputs[st.current_input].push(c);
                        }
                        KeyCode::Backspace => {
                            st.inputs[st.current_input].pop();
                        }
                        _ => {}
                    },
                    AppState::ManageSilos => match key.code {
                        KeyCode::Esc => {
                            app.state = AppState::Main;
                        }
                        KeyCode::Char('a') => {
                            app.state = AppState::AddSilo {
                                input: String::new(),
                            };
                        }
                        KeyCode::Enter => {
                            let selected_silo = if let Some(cfg) = &app.config {
                                if let Some(i) = app.silo_list_state.selected() {
                                    cfg.known_silos.get(i).cloned()
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            if let Some(silo_id) = selected_silo {
                                app.silo_id = silo_id.clone();
                                if app.rebuild_efs().await.is_err() {
                                    app.state = AppState::PromptInitSilo {
                                        silo_id: silo_id.clone(),
                                    };
                                } else {
                                    app.state = AppState::Main;
                                }
                            }
                        }
                        KeyCode::Char('d') => {
                            if let Some(i) = app.silo_list_state.selected() {
                                if let Some(cfg) = &mut app.config {
                                    if i < cfg.known_silos.len() {
                                        cfg.known_silos.remove(i);
                                        let _ = app.save_config();
                                    }
                                }
                            }
                        }
                        KeyCode::Down => {
                            if let Some(cfg) = &app.config {
                                let i = match app.silo_list_state.selected() {
                                    Some(i) if !cfg.known_silos.is_empty() => {
                                        (i + 1) % cfg.known_silos.len()
                                    }
                                    _ => 0,
                                };
                                if !cfg.known_silos.is_empty() {
                                    app.silo_list_state.select(Some(i));
                                }
                            }
                        }
                        KeyCode::Up => {
                            if let Some(cfg) = &app.config {
                                let i = match app.silo_list_state.selected() {
                                    Some(i) if !cfg.known_silos.is_empty() => {
                                        (i + cfg.known_silos.len() - 1) % cfg.known_silos.len()
                                    }
                                    _ => 0,
                                };
                                if !cfg.known_silos.is_empty() {
                                    app.silo_list_state.select(Some(i));
                                }
                            }
                        }
                        _ => {}
                    },
                    AppState::AddSilo { input } => match key.code {
                        KeyCode::Esc => {
                            app.state = AppState::ManageSilos;
                        }
                        KeyCode::Enter => {
                            let silo_id = input.clone();
                            if let Err(e) = app.add_silo(&silo_id) {
                                app.state = AppState::Error(e.to_string());
                            } else {
                                app.silo_id = silo_id;
                                if app.rebuild_efs().await.is_err() {
                                    app.state = AppState::PromptInitSilo {
                                        silo_id: app.silo_id.clone(),
                                    };
                                } else {
                                    app.state = AppState::Main;
                                }
                            }
                        }
                        KeyCode::Char(c) => {
                            input.push(c);
                        }
                        KeyCode::Backspace => {
                            input.pop();
                        }
                        _ => {}
                    },
                    AppState::PromptInitSilo { silo_id } => match key.code {
                        KeyCode::Char('y') | KeyCode::Enter => {
                            let sid = silo_id.clone();
                            if let Err(e) = app.initialize_new_silo(&sid).await {
                                app.state = AppState::Error(e.to_string());
                            } else {
                                app.silo_id = sid;
                                let _ = app.rebuild_efs().await;
                                app.state = AppState::Main;
                            }
                        }
                        KeyCode::Char('n') | KeyCode::Esc => {
                            app.state = AppState::ManageSilos;
                        }
                        _ => {}
                    },
                    AppState::Upload {
                        local_path,
                        remote_path,
                        current_input,
                    } => match key.code {
                        KeyCode::Esc => app.state = AppState::Main,
                        KeyCode::Tab | KeyCode::Down | KeyCode::Up => {
                            *current_input = 1 - *current_input;
                        }
                        KeyCode::Enter => {
                            let lp = local_path.clone();
                            let rp = remote_path.clone();
                            if let Some(efs) = &mut app.efs {
                                if let Err(e) = efs.put_recursive(&lp, &rp).await {
                                    app.state = AppState::Error(e.to_string());
                                } else {
                                    let _ = app.refresh_files().await;
                                    app.state = AppState::Main;
                                }
                            }
                        }
                        KeyCode::Char(c) => {
                            if *current_input == 0 {
                                local_path.push(c);
                            } else {
                                remote_path.push(c);
                            }
                        }
                        KeyCode::Backspace => {
                            if *current_input == 0 {
                                local_path.pop();
                            } else {
                                remote_path.pop();
                            }
                        }
                        _ => {}
                    },
                    AppState::Move {
                        source_path,
                        dest_path,
                    } => match key.code {
                        KeyCode::Esc => app.state = AppState::Main,
                        KeyCode::Enter => {
                            let src = source_path.clone();
                            let dst = dest_path.clone();

                            app.state = AppState::Moving;
                            // Force draw to show "Moving..."
                            terminal.draw(|f| ui(f, app))?;

                            if let Some(efs) = &mut app.efs {
                                // "logically deletes the data and uploads it"
                                match efs.get(&src).await {
                                    Ok(data) => {
                                        if let Err(e) = efs.put(&dst, &data).await {
                                            app.state = AppState::Error(e.to_string());
                                        } else {
                                            if let Err(e) = efs.delete(&src).await {
                                                app.state = AppState::Error(format!(
                                                    "Failed to delete source after upload: {}",
                                                    e
                                                ));
                                            } else {
                                                let _ = app.refresh_files().await;
                                                app.state = AppState::Main;
                                            }
                                        }
                                    }
                                    Err(e) => app.state = AppState::Error(e.to_string()),
                                }
                            }
                        }
                        KeyCode::Char(c) => {
                            dest_path.push(c);
                        }
                        KeyCode::Backspace => {
                            dest_path.pop();
                        }
                        _ => {}
                    },
                    AppState::Download {
                        remote_path,
                        local_path,
                    } => match key.code {
                        KeyCode::Esc => app.state = AppState::Main,
                        KeyCode::Enter => {
                            let rp = remote_path.clone();
                            let lp = local_path.clone();
                            if let Some(efs) = &app.efs {
                                match efs.get(&rp).await {
                                    Ok(data) => {
                                        if let Err(e) = std::fs::write(&lp, data) {
                                            app.state = AppState::Error(e.to_string());
                                        } else {
                                            app.state = AppState::Main;
                                        }
                                    }
                                    Err(e) => app.state = AppState::Error(e.to_string()),
                                }
                            }
                        }
                        KeyCode::Char(c) => {
                            local_path.push(c);
                        }
                        KeyCode::Backspace => {
                            local_path.pop();
                        }
                        _ => {}
                    },
                    AppState::ConfirmDelete { path } => match key.code {
                        KeyCode::Esc | KeyCode::Char('n') => app.state = AppState::Main,
                        KeyCode::Char('y') | KeyCode::Enter => {
                            let p = path.clone();
                            app.state = AppState::Deleting;
                            // Force draw to show "Deleting..."
                            terminal.draw(|f| ui(f, app))?;

                            if let Some(efs) = &mut app.efs {
                                if let Err(e) = efs.delete_recursive(&p).await {
                                    app.state = AppState::Error(e.to_string());
                                } else {
                                    let _ = app.refresh_files().await;
                                    app.state = AppState::Main;
                                }
                            }
                        }
                        _ => {}
                    },
                    AppState::Deleting | AppState::Moving => {}
                    AppState::Error(_) => match key.code {
                        KeyCode::Esc | KeyCode::Enter => {
                            app.state = AppState::Login;
                        }
                        _ => {}
                    },
                }
            }
        }
        if last_tick.elapsed() >= tick_rate {
            last_tick = Instant::now();
        }
    }
}

fn ui(f: &mut Frame, app: &mut App) {
    let size = f.size();

    match &app.state {
        AppState::Login => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(5)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ]
                    .as_ref(),
                )
                .split(size);

            let title =
                Paragraph::new("EFS TUI - Login").block(Block::default().borders(Borders::ALL));
            f.render_widget(title, chunks[0]);

            let password_display = "*".repeat(app.password.len());
            let password_input = Paragraph::new(password_display)
                .block(Block::default().borders(Borders::ALL).title("Password"));
            f.render_widget(password_input, chunks[1]);
        }
        AppState::Main => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Min(0),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(size);

            let header_text = if app.silo_id.is_empty() {
                format!("No Silo Selected | Config: {}", app.config_path)
            } else {
                format!(
                    "Silo: {} | Path: {} | Config: {}",
                    app.silo_id, app.current_dir, app.config_path
                )
            };
            let header = Paragraph::new(header_text).block(Block::default().borders(Borders::ALL));
            f.render_widget(header, chunks[0]);

            if app.silo_id.is_empty() {
                let msg = Paragraph::new("No silo selected. Press 's' to manage silos.")
                    .block(Block::default().borders(Borders::ALL));
                f.render_widget(msg, chunks[1]);
            } else {
                let current_view = app.get_current_view();
                let items: Vec<ListItem> = current_view
                    .iter()
                    .map(|item| match item {
                        FileItem::Dir(name) => ListItem::new(format!("[DIR] {}/", name)),
                        FileItem::File(name) => ListItem::new(format!("      {}", name)),
                    })
                    .collect();
                let file_list = List::new(items)
                    .block(Block::default().borders(Borders::ALL).title("Files"))
                    .highlight_style(
                        ratatui::style::Style::default()
                            .add_modifier(ratatui::style::Modifier::REVERSED),
                    );
                f.render_stateful_widget(file_list, chunks[1], &mut app.file_list_state);
            }

            let footer_text = if app.efs.is_some() {
                "q: quit | b: backends | s: silos | r: refresh | u: upload | d: download | m: move | x: delete | enter: open dir"
            } else {
                "q: quit | b: backends | s: silos"
            };
            let footer = Paragraph::new(footer_text).block(Block::default().borders(Borders::ALL));
            f.render_widget(footer, chunks[2]);
        }
        AppState::Move {
            source_path,
            dest_path,
        } => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(5)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ]
                    .as_ref(),
                )
                .split(size);

            let src_widget = Paragraph::new(source_path.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Source Path (Internal)"),
            );
            let dst_widget = Paragraph::new(dest_path.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Destination Path (Internal)")
                    .border_style(
                        ratatui::style::Style::default().fg(ratatui::style::Color::Yellow),
                    ),
            );

            f.render_widget(src_widget, chunks[0]);
            f.render_widget(dst_widget, chunks[1]);
            f.render_widget(
                Paragraph::new("Enter: Move (Upload & Delete) | Esc: Cancel"),
                chunks[2],
            );
        }
        AppState::ManageBackends => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Min(0),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(size);

            let header =
                Paragraph::new("Manage Backends").block(Block::default().borders(Borders::ALL));
            f.render_widget(header, chunks[0]);

            let items: Vec<ListItem> = if let Some(cfg) = &app.config {
                cfg.backends
                    .iter()
                    .map(|b| {
                        let info = match &b.backend_type {
                            BackendType::Local { path } => format!("Local: {}", path),
                            BackendType::S3 { bucket, .. } => format!("S3: {}", bucket),
                        };
                        ListItem::new(format!("{} ({})", b.name, info))
                    })
                    .collect()
            } else {
                vec![]
            };

            let backend_list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title("Backends"))
                .highlight_style(
                    ratatui::style::Style::default()
                        .add_modifier(ratatui::style::Modifier::REVERSED),
                );
            f.render_stateful_widget(backend_list, chunks[1], &mut app.backend_list_state);

            let footer =
                Paragraph::new("esc: back | a: add local | s: add s3 | d: delete selected")
                    .block(Block::default().borders(Borders::ALL));
            f.render_widget(footer, chunks[2]);
        }
        AppState::AddBackend(st) => {
            let labels = match st.backend_type {
                NewBackendType::Local => vec!["Name", "Path"],
                NewBackendType::S3 => vec![
                    "Name",
                    "Bucket",
                    "Region",
                    "Access Key (optional)",
                    "Secret Key (optional)",
                ],
            };

            let mut constraints = vec![Constraint::Length(3); labels.len()];
            constraints.push(Constraint::Min(0));

            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(2)
                .constraints(constraints)
                .split(size);

            for (i, label) in labels.iter().enumerate() {
                let style = if i == st.current_input {
                    ratatui::style::Style::default().fg(ratatui::style::Color::Yellow)
                } else {
                    ratatui::style::Style::default()
                };
                let input = Paragraph::new(st.inputs[i].as_str()).block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(*label)
                        .border_style(style),
                );
                f.render_widget(input, chunks[i]);
            }

            let help = Paragraph::new("Enter: save | Tab/Arrows: switch fields | Esc: cancel")
                .block(Block::default());
            f.render_widget(help, chunks[labels.len()]);
        }
        AppState::ManageSilos => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Min(0),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(size);

            let header =
                Paragraph::new("Manage Silos").block(Block::default().borders(Borders::ALL));
            f.render_widget(header, chunks[0]);

            let items: Vec<ListItem> = if let Some(cfg) = &app.config {
                cfg.known_silos
                    .iter()
                    .map(|s| {
                        let indicator = if s == &app.silo_id { " [*]" } else { "" };
                        ListItem::new(format!("{}{}", s, indicator))
                    })
                    .collect()
            } else {
                vec![]
            };

            let silo_list = List::new(items)
                .block(Block::default().borders(Borders::ALL).title("Silos"))
                .highlight_style(
                    ratatui::style::Style::default()
                        .add_modifier(ratatui::style::Modifier::REVERSED),
                );
            f.render_stateful_widget(silo_list, chunks[1], &mut app.silo_list_state);

            let footer = Paragraph::new(
                "esc: back | a: add/init silo | enter: switch to selected | d: forget selected",
            )
            .block(Block::default().borders(Borders::ALL));
            f.render_widget(footer, chunks[2]);
        }
        AppState::AddSilo { input } => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(5)
                .constraints([Constraint::Length(3), Constraint::Min(0)].as_ref())
                .split(size);

            let input_widget = Paragraph::new(input.as_str())
                .block(Block::default().borders(Borders::ALL).title("New Silo ID"));
            f.render_widget(input_widget, chunks[0]);

            let help = Paragraph::new("Enter: add silo | Esc: cancel").block(Block::default());
            f.render_widget(help, chunks[1]);
        }
        AppState::PromptInitSilo { silo_id } => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(5)
                .constraints([Constraint::Length(7), Constraint::Min(0)].as_ref())
                .split(size);

            let block = Block::default()
                .borders(Borders::ALL)
                .title("Silo Not Found");
            let paragraph = Paragraph::new(format!(
                "\nSilo '{}' was not found in storage.\n\nWould you like to initialize it?",
                silo_id
            ))
            .block(block)
            .alignment(ratatui::layout::Alignment::Center);
            f.render_widget(paragraph, chunks[0]);

            let help = Paragraph::new("y / Enter: Yes, initialize | n / Esc: No, cancel")
                .block(Block::default())
                .alignment(ratatui::layout::Alignment::Center);
            f.render_widget(help, chunks[1]);
        }
        AppState::Upload {
            local_path,
            remote_path,
            current_input,
        } => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(5)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ]
                    .as_ref(),
                )
                .split(size);

            let lp_style = if *current_input == 0 {
                ratatui::style::Style::default().fg(ratatui::style::Color::Yellow)
            } else {
                ratatui::style::Style::default()
            };
            let rp_style = if *current_input == 1 {
                ratatui::style::Style::default().fg(ratatui::style::Color::Yellow)
            } else {
                ratatui::style::Style::default()
            };

            let lp_widget = Paragraph::new(local_path.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Local Path")
                    .border_style(lp_style),
            );
            let rp_widget = Paragraph::new(remote_path.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Remote Path")
                    .border_style(rp_style),
            );

            f.render_widget(lp_widget, chunks[0]);
            f.render_widget(rp_widget, chunks[1]);
            f.render_widget(
                Paragraph::new("Enter: upload | Tab: switch field | Esc: cancel"),
                chunks[2],
            );
        }
        AppState::Download {
            remote_path,
            local_path,
        } => {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(5)
                .constraints(
                    [
                        Constraint::Length(3),
                        Constraint::Length(3),
                        Constraint::Min(0),
                    ]
                    .as_ref(),
                )
                .split(size);

            let rp_widget = Paragraph::new(remote_path.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Remote Path (Selected)"),
            );
            let lp_widget = Paragraph::new(local_path.as_str()).block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Local Destination Path")
                    .border_style(
                        ratatui::style::Style::default().fg(ratatui::style::Color::Yellow),
                    ),
            );

            f.render_widget(rp_widget, chunks[0]);
            f.render_widget(lp_widget, chunks[1]);
            f.render_widget(Paragraph::new("Enter: download | Esc: cancel"), chunks[2]);
        }
        AppState::ConfirmDelete { path } => {
            let block = Block::default()
                .borders(Borders::ALL)
                .title("Confirm Delete");
            let paragraph =
                Paragraph::new(format!("Are you sure you want to delete '{}'? (y/n)", path))
                    .block(block);
            f.render_widget(paragraph, size);
        }
        AppState::Deleting => {
            let block = Block::default().borders(Borders::ALL).title("Deleting");
            let paragraph =
                Paragraph::new("Deleting directory recursively... please wait.").block(block);
            f.render_widget(paragraph, size);
        }
        AppState::Moving => {
            let block = Block::default().borders(Borders::ALL).title("Moving");
            let paragraph =
                Paragraph::new("Moving data (downloading, uploading, deleting)... please wait.")
                    .block(block);
            f.render_widget(paragraph, size);
        }
        AppState::Error(msg) => {
            let block = Block::default().borders(Borders::ALL).title("Error");
            let paragraph = Paragraph::new(msg.as_str())
                .block(block)
                .wrap(ratatui::widgets::Wrap { trim: true });
            f.render_widget(paragraph, size);
        }
    }
}
