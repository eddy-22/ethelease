package main

import (
    "flag"
    "io/ioutil"
    "gopkg.in/yaml.v2"
    "os"
    "reflect"
    "text/template"
)

type Yaml map[string]string
type Yaml2 map[string]interface{}

func targetMapper(mapA map[string]string, sched interface{}) map[string]string {
    target := make(map[string]string)
    for k, v := range mapA {
        target[k] = v
    }
    rose := reflect.ValueOf(sched)
    for _, k := range rose.MapKeys() {
        v := rose.MapIndex(k).Interface().(string)
        keyed := k.Interface().(string)
        target[keyed] = v
    }
    return target
}

func procTemplate(inpath string, outpath string, values map[string]string) {
    tmpl, err_read := template.ParseFiles(inpath)
    check(err_read)
    file, err_file := os.Create(outpath)
    check(err_file)
    err := tmpl.Execute(file, values)
    check(err)
    file.Close()
}

func readInits(infile string) map[string]string {
    y := Yaml{}
    yamFile, err_read := ioutil.ReadFile(infile)
    check(err_read)
    err_yam := yaml.Unmarshal(yamFile, &y)
    check(err_yam)
    return y
}

func readSpecs(infile string) map[string]interface{} {
    y := Yaml2{}
    yamFile, err_read := ioutil.ReadFile(infile)
    check(err_read)
    err_yam := yaml.Unmarshal(yamFile, &y)
    check(err_yam)
    return y
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

func main() {

    env := flag.String("env", "dv", "environment")
    flag.Parse()

    regStr := "registry"
    schedStr := "scheduler"

    confs := readSpecs("./configs/specs.yaml")
    inits := readInits("./configs/inits.yaml")
    inSchedTmplPath := "./scheduler.yaml.tmpl"
    outSchedPath := "./scheduler.yaml"
    vals := map[string]string {
        "Env": *env,
        "Registry": inits[regStr],
    }
    schedVals := targetMapper(vals, confs[schedStr])
    procTemplate(
        inSchedTmplPath,
        outSchedPath,
        schedVals,
    )

}
