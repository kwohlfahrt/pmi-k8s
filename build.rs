use std::env;
use std::path::PathBuf;

fn main() {
    let pmix = pkg_config::Config::new()
        .atleast_version("5.0")
        .probe("pmix")
        .expect("Unable to locate PMIx library with pkg-config.");

    let builder = bindgen::Builder::default()
        .header_contents(
            "wrapper.h",
            r#"
            #include <pmix.h>
            #include <pmix_server.h>
            "#,
        )
        .clang_args(
            pmix.include_paths
                .iter()
                .map(|p| format!("-I{}", p.display())),
        )
        .no_copy("pmix_(info|value)(_t)?")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));

    let bindings = builder
        .generate()
        .expect("Unable to generate PMIx bindings.");

    let out_path =
        PathBuf::from(env::var("OUT_DIR").expect("Unable to read OUT_DIR environment variable."));
    bindings
        .write_to_file(out_path.join("bindings_pmix.rs"))
        .expect("Unable to write pmix bindings.");
}
