import { notification } from 'antd';
import { Modal } from 'antd';
const confirm = Modal.confirm;
export default {
  confirm(msg, title, onOk, onCancel) {
    confirm({
      title: title,
      content: msg,
      onOk: () => {
        onOk && onOk();
      },
      onCancel: () => {
        onCancel && onCancel();
      }
    })
  }
}