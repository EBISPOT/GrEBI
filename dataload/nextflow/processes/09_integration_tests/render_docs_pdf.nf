// Render the documentation PDF from the self-contained HTML produced by
// test_query_templates (generate_docs_pdf.mjs --output grebi-docs.html).
//
// This runs in the upstream-maintained puppeteer image so that chromium does
// not need to be installed in any GrEBI image. The HTML embeds all images and
// CSS as data URIs, so no live stack is required here — only chromium.
process render_docs_pdf {
    container 'ghcr.io/puppeteer/puppeteer:25.2.0'

    memory "2 GB"
    time "30m"

    publishDir "${out_dir}", overwrite: true

    input:
    path(docs_html)
    path(render_script)
    val(out_dir)

    output:
    path("grebi-docs.pdf")

    script:
    """
    # chrome lives in the image's (read-only) cache; give it a writable HOME.
    export HOME=\$PWD
    export PUPPETEER_CACHE_DIR=/home/pptruser/.cache/puppeteer
    node ${render_script} --html ${docs_html} --output grebi-docs.pdf
    """
}
