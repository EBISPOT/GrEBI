import PropVal from "../../model/PropVal";

export default function isSingleLineProp(prop:PropVal) {
    return prop.value.toString().length <= 48;
}
