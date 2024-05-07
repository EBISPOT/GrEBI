export default class Reified<T> {
  value: T;
  datasources: string[];
  props: any;

  private constructor(value:T,datasources:string[],props:any) {
      this.value = value;
      this.datasources = datasources;
      this.props = props;
  }

  public static from<T>(src:any):Reified<T> {

    if(!src) {
      return src;
    }

    if(typeof src === 'object') {

      let obj = Object.assign({}, src);
      delete obj['grebi:value'];
      delete obj['grebi:datasources'];

      return new Reified<T>(src['grebi:value'],src['grebi:datasources'],src)

    } else {
      return new Reified<T>(src,[], {});
    }

  }
}
